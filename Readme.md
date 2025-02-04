# Tool Sinkronisasi Struktur Database

Tool berbasis Python untuk menyinkronkan struktur database antara database sumber dan target MySQL, dirancang khusus untuk migrasi database SIAKADKU.

## Fitur

- Membandingkan struktur database antara database sumber dan target
- Membuat tabel yang belum ada secara otomatis
- Menyinkronkan struktur dan kolom tabel
- Menangani foreign key constraints
- Penanganan khusus untuk tabel `dosen` dan `dosen_wali_prodi`
- Sistem logging detail

## Prasyarat

- Python 3.6+
- MySQL Server
- MySQL Connector Python

## Instalasi

1. Install package yang dibutuhkan:
```bash
pip install mysql-connector-python

Copy

Apply

README.md
Konfigurasi
Edit config.py dengan kredensial database Anda:

source_db = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'iims_1',
    'port': 3306
}

target_db = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'siakadku-old',
    'port': 3306
}

Copy

Apply

Cara Penggunaan
Jalankan tool perbandingan:
python db_comparison.py

Copy

Execute

Tool akan:

Menampilkan perbedaan antar database
Menampilkan daftar tabel yang hilang
Menampilkan perbedaan struktur
Meminta konfirmasi untuk sinkronisasi
Log otomatis dibuat dengan format: db_sync_YYYYMMDD_HHMMSS.log

Catatan Penting
Selalu backup database sebelum melakukan sinkronisasi
Tool akan menonaktifkan foreign key checks selama sinkronisasi
Penanganan khusus diterapkan untuk tabel dosen dan dosen_wali_prodi
Semua operasi dicatat dalam log untuk pelacakan dan debugging
Struktur File
├── config.py           # Pengaturan konfigurasi database
├── db_comparison.py    # Logika utama sinkronisasi
└── *.log              # File log yang dihasilkan

Copy

Execute

Penanganan Error
Error koneksi dicatat dan dilaporkan
Kegagalan pembuatan tabel dicatat dengan pesan error detail
Masalah foreign key constraint ditangani secara otomatis