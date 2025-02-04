import mysql.connector
from mysql.connector import Error
import sys
from typing import Dict, List, Tuple
import logging
from datetime import datetime
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'db_sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class DatabaseComparator:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_config = source_config
        self.target_config = target_config
        self.source_conn = None
        self.target_conn = None

    def connect(self) -> bool:
        """Establish connections to both databases"""
        try:
            self.source_conn = mysql.connector.connect(**self.source_config)
            self.target_conn = mysql.connector.connect(**self.target_config)
            return True
        except Error as e:
            logging.error(f"Error connecting to databases: {e}")
            return False

    def get_table_structure(self, connection, table_name: str) -> List[Dict]:
        """Get column information for a specific table"""
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = cursor.fetchall()
            cursor.close()
            return columns
        except Error as e:
            logging.error(f"Error getting structure for table {table_name}: {e}")
            return []

    def get_tables(self, connection) -> List[str]:
        """Get all tables from a database"""
        try:
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            cursor.close()
            return tables
        except Error as e:
            logging.error(f"Error getting tables: {e}")
            return []

    def handle_dosen_tables(self) -> bool:
        """Handle special cases for dosen and dosen_wali_prodi tables"""
        try:
            cursor = self.source_conn.cursor()
            
            # 1. Drop existing constraints for dosen_wali_prodi
            logging.info("Dropping existing constraints...")
            try:
                cursor.execute("""
                    SELECT CONSTRAINT_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'dosen_wali_prodi'
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """)
                constraints = cursor.fetchall()
                for (constraint_name,) in constraints:
                    cursor.execute(f"ALTER TABLE dosen_wali_prodi DROP FOREIGN KEY `{constraint_name}`")
            except Error as e:
                logging.info(f"No constraints to drop: {e}")

            # 2. Fix dosen table - Modified index creation
            logging.info("Fixing dosen table structure...")
            try:
                # Check if index exists first
                cursor.execute("""
                    SELECT COUNT(1) 
                    FROM information_schema.STATISTICS 
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'dosen' 
                    AND INDEX_NAME = 'idx_dosen_nidn'
                """)
                index_exists = cursor.fetchone()[0] > 0
                
                if not index_exists:
                    cursor.execute("ALTER TABLE dosen ADD UNIQUE INDEX idx_dosen_nidn (nidn)")
            except Error as e:
                if "Duplicate" not in str(e):
                    raise e

            # 3. Drop and recreate dosen_wali_prodi
            logging.info("Recreating dosen_wali_prodi table...")
            cursor.execute("DROP TABLE IF EXISTS dosen_wali_prodi")
            
            cursor.execute("""
                CREATE TABLE dosen_wali_prodi (
                    id int NOT NULL AUTO_INCREMENT,
                    nidn varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
                    prodi_id int DEFAULT NULL,
                    tahun_ajar varchar(9) DEFAULT NULL,
                    created_at datetime DEFAULT CURRENT_TIMESTAMP,
                    updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY unique_prodi_tahun (prodi_id,tahun_ajar),
                    KEY nidn (nidn)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            """)

            # 4. Add foreign keys
            logging.info("Adding foreign key constraints...")
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            cursor.execute("""
                ALTER TABLE dosen_wali_prodi
                ADD CONSTRAINT dosen_wali_prodi_ibfk_1 
                FOREIGN KEY (nidn) REFERENCES dosen (nidn)
            """)

            cursor.execute("""
                ALTER TABLE dosen_wali_prodi
                ADD CONSTRAINT dosen_wali_prodi_ibfk_2 
                FOREIGN KEY (prodi_id) REFERENCES prodi (id_prodi)
            """)

            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            
            self.source_conn.commit()
            logging.info("Successfully handled dosen and dosen_wali_prodi tables")
            return True

        except Error as e:
            logging.error(f"Error handling dosen tables: {e}")
            try:
                self.source_conn.rollback()
            except:
                pass
            return False

    def generate_alter_statements(self, table_name: str, source_cols: List[Dict], 
                                target_cols: List[Dict]) -> List[str]:
        """Generate ALTER TABLE statements to sync the structures"""
        alter_statements = []
        source_col_names = {col['Field'] for col in source_cols}
        target_col_names = {col['Field'] for col in target_cols}

        # Add missing columns
        for col in target_cols:
            if col['Field'] not in source_col_names:
                stmt = f"ALTER TABLE {table_name} ADD COLUMN {col['Field']} {col['Type']}"
                if col['Null'] == 'NO':
                    stmt += " NOT NULL"
                if col['Default'] is not None:
                    if col['Type'].lower() in ('timestamp', 'datetime') and \
                       col['Default'].upper() == 'CURRENT_TIMESTAMP':
                        stmt += " DEFAULT CURRENT_TIMESTAMP"
                    elif 'char' in col['Type'].lower() or 'text' in col['Type'].lower():
                        stmt += f" DEFAULT '{col['Default']}'"
                    else:
                        stmt += f" DEFAULT {col['Default']}"
                alter_statements.append(stmt)

        return alter_statements

    def compare_structures(self) -> Tuple[List[str], List[str], Dict]:
        """Compare database structures"""
        if not self.connect():
            return [], [], {}

        source_tables = set(self.get_tables(self.source_conn))
        target_tables = set(self.get_tables(self.target_conn))

        missing_in_source = list(target_tables - source_tables)
        missing_in_target = list(source_tables - target_tables)
        
        different_structures = {}
        common_tables = source_tables.intersection(target_tables)

        for table in common_tables:
            source_structure = self.get_table_structure(self.source_conn, table)
            target_structure = self.get_table_structure(self.target_conn, table)

            if source_structure != target_structure:
                different_structures[table] = {
                    'source': source_structure,
                    'target': target_structure
                }

        return missing_in_source, missing_in_target, different_structures

    def sync_structures(self) -> bool:
        """Synchronize the database structures"""
        missing_in_source, _, different_structures = self.compare_structures()

        if not (missing_in_source or different_structures):
            logging.info("No structural differences found that need synchronization.")
            return True

        try:
            cursor = self.source_conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Handle dosen tables first if needed
            if any(table in {'dosen', 'dosen_wali_prodi'} for table in missing_in_source) or \
            any(table in {'dosen', 'dosen_wali_prodi'} for table in different_structures):
                if not self.handle_dosen_tables():
                    return False

            # Handle missing tables
            for table in missing_in_source:
                if table not in {'dosen', 'dosen_wali_prodi'}:
                    logging.info(f"Creating table {table} in source database")
                    target_cursor = self.target_conn.cursor()
                    target_cursor.execute(f"SHOW CREATE TABLE {table}")
                    create_stmt = target_cursor.fetchone()[1]
                    
                    logging.info(f"Original CREATE TABLE statement:\n{create_stmt}")
                    
                    # First remove foreign key constraints
                    create_stmt = re.sub(
                        r',\s*CONSTRAINT\s+`?\w+`?\s+FOREIGN\s+KEY\s+\([^)]+\)\s+REFERENCES\s+`?\w+`?\s*\([^)]+\)(\s+ON\s+DELETE\s+[A-Z]+)?(\s+ON\s+UPDATE\s+[A-Z]+)?',
                        '',
                        create_stmt
                    )
                    
                    # Handle the table options more carefully
                    # Split the statement into the main body and the table options
                    parts = create_stmt.rsplit(')', 1)
                    if len(parts) == 2:
                        main_body, table_options = parts
                        # Clean up table options
                        table_options = re.sub(r'AUTO_INCREMENT=\d+\s*', '', table_options.strip())
                        # Reconstruct the statement
                        create_stmt = f"{main_body}) {table_options}"
                    
                    logging.info(f"Modified CREATE TABLE statement:\n{create_stmt}")
                    
                    try:
                        cursor.execute(create_stmt)
                    except Error as e:
                        logging.error(f"Error creating table {table}: {e}")
                        logging.error(f"Failed CREATE TABLE statement:\n{create_stmt}")
                        raise e

                    # Add foreign key constraints separately
                    target_cursor.execute("""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME,
                            CONSTRAINT_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    """, (self.target_config['database'], table))
                    
                    for col, ref_table, ref_col, constraint_name in target_cursor.fetchall():
                        try:
                            # Get the original foreign key definition to preserve ON DELETE/UPDATE actions
                            target_cursor.execute("""
                                SELECT DELETE_RULE, UPDATE_RULE
                                FROM information_schema.REFERENTIAL_CONSTRAINTS
                                WHERE CONSTRAINT_SCHEMA = %s AND CONSTRAINT_NAME = %s
                            """, (self.target_config['database'], constraint_name))
                            
                            delete_rule, update_rule = target_cursor.fetchone() or ('RESTRICT', 'RESTRICT')
                            
                            fk_stmt = f"""
                                ALTER TABLE {table}
                                ADD CONSTRAINT `fk_{table}_{ref_table}_{col}`
                                FOREIGN KEY ({col})
                                REFERENCES {ref_table}({ref_col})
                            """
                            
                            if delete_rule != 'RESTRICT':
                                fk_stmt += f" ON DELETE {delete_rule}"
                            if update_rule != 'RESTRICT':
                                fk_stmt += f" ON UPDATE {update_rule}"
                                
                            cursor.execute(fk_stmt)
                            
                        except Error as e:
                            logging.error(f"Error adding foreign key for {table}.{col}: {e}")
                            if "Missing index" in str(e):
                                logging.info(f"Creating missing index for {table}.{col}")
                                cursor.execute(f"ALTER TABLE {table} ADD INDEX (`{col}`)")
                                cursor.execute(fk_stmt)

            # Rest of the synchronization code...
            # [Handle different structures code remains the same]
            
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            self.source_conn.commit()
            logging.info("Database synchronization completed successfully")
            return True

        except Error as e:
            logging.error(f"Error during synchronization: {e}")
            try:
                self.source_conn.rollback()
            except:
                pass
            return False
        """Synchronize the database structures"""
        missing_in_source, _, different_structures = self.compare_structures()

        if not (missing_in_source or different_structures):
            logging.info("No structural differences found that need synchronization.")
            return True

        try:
            cursor = self.source_conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Handle dosen tables first if needed
            if any(table in {'dosen', 'dosen_wali_prodi'} for table in missing_in_source) or \
            any(table in {'dosen', 'dosen_wali_prodi'} for table in different_structures):
                if not self.handle_dosen_tables():
                    return False

            # Handle missing tables
            for table in missing_in_source:
                if table not in {'dosen', 'dosen_wali_prodi'}:
                    logging.info(f"Creating table {table} in source database")
                    target_cursor = self.target_conn.cursor()
                    target_cursor.execute(f"SHOW CREATE TABLE {table}")
                    create_stmt = target_cursor.fetchone()[1]
                    
                    # Clean up the create table statement
                    # Remove AUTO_INCREMENT value but keep the ENGINE and CHARSET
                    create_stmt = re.sub(
                        r'(ENGINE=\w+)\s+AUTO_INCREMENT=\d+(\s+DEFAULT CHARSET=[\w\d]+)', 
                        r'\1\2', 
                        create_stmt
                    )
                    
                    # Remove foreign key constraints
                    create_stmt = re.sub(
                        r',\s*CONSTRAINT\s+`?\w+`?\s+FOREIGN\s+KEY\s+[^,]+REFERENCES\s+[^,]+\)', 
                        '', 
                        create_stmt
                    )
                    
                    try:
                        cursor.execute(create_stmt)
                    except Error as e:
                        logging.error(f"Error creating table {table}: {e}")
                        logging.debug(f"Failed CREATE TABLE statement: {create_stmt}")
                        raise e

                    # Add foreign key constraints separately
                    target_cursor.execute("""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    """, (self.target_config['database'], table))
                    
                    for col, ref_table, ref_col in target_cursor.fetchall():
                        try:
                            cursor.execute(f"""
                                ALTER TABLE {table}
                                ADD CONSTRAINT `fk_{table}_{ref_table}_{col}`
                                FOREIGN KEY ({col})
                                REFERENCES {ref_table}({ref_col})
                            """)
                        except Error as e:
                            logging.error(f"Error adding foreign key for {table}.{col}: {e}")
                            if "Missing index" in str(e):
                                logging.info(f"Creating missing index for {table}.{col}")
                                cursor.execute(f"ALTER TABLE {table} ADD INDEX (`{col}`)")
                                cursor.execute(f"""
                                    ALTER TABLE {table}
                                    ADD CONSTRAINT `fk_{table}_{ref_table}_{col}`
                                    FOREIGN KEY ({col})
                                    REFERENCES {ref_table}({ref_col})
                                """)

            # Handle different structures
            for table, structures in different_structures.items():
                if table not in {'dosen', 'dosen_wali_prodi'}:
                    logging.info(f"Synchronizing structure for table {table}")
                    alter_statements = self.generate_alter_statements(
                        table, 
                        structures['source'],
                        structures['target']
                    )
                    for stmt in alter_statements:
                        logging.info(f"Executing: {stmt}")
                        try:
                            cursor.execute(stmt)
                        except Error as e:
                            logging.error(f"Error executing ALTER statement: {e}")
                            raise e
            
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            self.source_conn.commit()
            logging.info("Database synchronization completed successfully")
            return True

        except Error as e:
            logging.error(f"Error during synchronization: {e}")
            try:
                self.source_conn.rollback()
            except:
                pass
            return False
        """Synchronize the database structures"""
        missing_in_source, _, different_structures = self.compare_structures()

        if not (missing_in_source or different_structures):
            logging.info("No structural differences found that need synchronization.")
            return True

        try:
            cursor = self.source_conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Handle dosen tables first if needed
            if any(table in {'dosen', 'dosen_wali_prodi'} for table in missing_in_source) or \
               any(table in {'dosen', 'dosen_wali_prodi'} for table in different_structures):
                if not self.handle_dosen_tables():
                    return False

            # Handle missing tables
            for table in missing_in_source:
                if table not in {'dosen', 'dosen_wali_prodi'}:
                    logging.info(f"Creating table {table} in source database")
                    target_cursor = self.target_conn.cursor()
                    target_cursor.execute(f"SHOW CREATE TABLE {table}")
                    create_stmt = target_cursor.fetchone()[1]
                    
                    # Remove foreign key constraints from create statement
                    create_stmt = re.sub(
                        r',\s*CONSTRAINT\s+`?\w+`?\s+FOREIGN\s+KEY\s+[^,]+REFERENCES\s+[^,]+\)', 
                        '', 
                        create_stmt
                    )
                    
                    try:
                        cursor.execute(create_stmt)
                    except Error as e:
                        logging.error(f"Error creating table {table}: {e}")
                        raise e

                    # Add foreign key constraints separately
                    target_cursor.execute("""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = %s
                        AND TABLE_NAME = %s
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    """, (self.target_config['database'], table))
                    
                    for col, ref_table, ref_col in target_cursor.fetchall():
                        try:
                            cursor.execute(f"""
                                ALTER TABLE {table}
                                ADD CONSTRAINT `fk_{table}_{ref_table}_{col}`
                                FOREIGN KEY ({col})
                                REFERENCES {ref_table}({ref_col})
                            """)
                        except Error as e:
                            logging.error(f"Error adding foreign key for {table}.{col}: {e}")
                            if "Missing index" in str(e):
                                logging.info(f"Creating missing index for {table}.{col}")
                                cursor.execute(f"ALTER TABLE {table} ADD INDEX (`{col}`)")
                                cursor.execute(f"""
                                    ALTER TABLE {table}
                                    ADD CONSTRAINT `fk_{table}_{ref_table}_{col}`
                                    FOREIGN KEY ({col})
                                    REFERENCES {ref_table}({ref_col})
                                """)

            # Alter existing tables
            for table, structures in different_structures.items():
                if table not in {'dosen', 'dosen_wali_prodi'}:
                    logging.info(f"Synchronizing structure for table {table}")
                    alter_statements = self.generate_alter_statements(
                        table, 
                        structures['source'],
                        structures['target']
                    )
                    for stmt in alter_statements:
                        logging.info(f"Executing: {stmt}")
                        try:
                            cursor.execute(stmt)
                        except Error as e:
                            logging.error(f"Error executing ALTER statement: {e}")
                            raise e
            
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            self.source_conn.commit()
            logging.info("Database synchronization completed successfully")
            return True

            self.source_conn.commit()
            logging.info("Database synchronization completed successfully")
            return True

        except Error as e:
            logging.error(f"Error during synchronization: {e}")
            try:
                self.source_conn.rollback()
            except:
                pass
            return False

    def close_connections(self):
        """Close database connections"""
        if self.source_conn and self.source_conn.is_connected():
            self.source_conn.close()
        if self.target_conn and self.target_conn.is_connected():
            self.target_conn.close()

def main():
    # Import database configurations
    try:
        from config import source_db, target_db
    except ImportError:
        logging.error("File config.py tidak ditemukan. Pastikan file tersebut ada di direktori yang sama.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error saat membaca konfigurasi: {e}")
        sys.exit(1)

    # Create database comparator instance
    comparator = DatabaseComparator(source_db, target_db)
    
    # Compare structures and print differences
    missing_in_source, missing_in_target, different_structures = comparator.compare_structures()

    if missing_in_source:
        logging.info("Tables missing in source database:")
        for table in missing_in_source:
            logging.info(f"- {table}")

    if missing_in_target:
        logging.info("Tables missing in target database:")
        for table in missing_in_target:
            logging.info(f"- {table}")

    if different_structures:
        logging.info("Tables with different structures:")
        for table in different_structures:
            logging.info(f"- {table}")

    # Perform synchronization if user confirms
    if input("Do you want to synchronize the databases? (y/n): ").lower() == 'y':
        if comparator.sync_structures():
            logging.info("Synchronization completed successfully")
        else:
            logging.error("Synchronization failed")

if __name__ == "__main__":
    main()