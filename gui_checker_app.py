import sys
import os
import pandas as pd
import geopandas as gpd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QTextEdit, QFileDialog, QMessageBox,
    QProgressBar
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from pathlib import Path

# --- Worker Thread untuk proses pengecekan ---
class CheckWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, gpkg_path, excel_path):
        super().__init__()
        self.gpkg_path = gpkg_path
        self.excel_path = excel_path

    def run(self):
        try:
            # Validasi file exists
            if not Path(self.excel_path).exists():
                self.error.emit("File Excel tidak ditemukan")
                return
            if not Path(self.gpkg_path).exists():
                self.error.emit("File GeoPackage tidak ditemukan")
                return

            # --- LANGKAH 1: Baca file Excel ---
            self.progress.emit("Membaca file Master Excel...")
            df_master = pd.read_excel(self.excel_path, dtype={'idsubsls': str, 'kdsubsls': str, 'nmsls': str})
            master_data = {}
            master_digit_issues = []
            
            for index, row in df_master.iterrows():
                idsubsls_key = str(row['idsubsls']).strip()
                kdsubsls_master = str(row['kdsubsls']).strip()
                
                # Cek digit kdsubsls di Excel
                if kdsubsls_master != '' and kdsubsls_master != '00':
                    if len(kdsubsls_master) > 2:
                        master_digit_issues.append(f"Excel - ID: {idsubsls_key}: kdsubsls '{kdsubsls_master}' lebih dari 2 digit")
                    elif len(kdsubsls_master) == 1:
                        master_digit_issues.append(f"Excel - ID: {idsubsls_key}: kdsubsls '{kdsubsls_master}' hanya 1 digit")
                
                # Normalisasi kdsubsls untuk penyimpanan
                kdsubsls_normalized = kdsubsls_master
                if kdsubsls_master == '0':
                    kdsubsls_normalized = '00'
                elif len(kdsubsls_master) == 1 and kdsubsls_master != '0':
                    kdsubsls_normalized = f"0{kdsubsls_master}"
                
                master_data[idsubsls_key] = {
                    'nmsls': str(row['nmsls']).strip(),
                    'kdsubsls': kdsubsls_normalized,  # Simpan yang sudah dinormalisasi
                    'kdsubsls_original': kdsubsls_master  # Simpan versi original untuk comparison
                }
            
            self.progress.emit(f"Berhasil memuat {len(master_data)} baris dari Master Excel.")
            if master_digit_issues:
                self.progress.emit(f"Peringatan: Ditemukan {len(master_digit_issues)} issue digit kdsubsls di Excel")

            # --- LANGKAH 2: Baca file GeoPackage ---
            self.progress.emit("Membaca file GeoPackage...")
            gdf = gpd.read_file(self.gpkg_path, dtype={'idsubsls': str, 'kdsubsls': str, 'nmsls': str})
            self.progress.emit(f"Berhasil memuat {len(gdf)} baris dari GeoPackage.")

            all_results = []
            gpkg_idsubsls_set = set()
            gpkg_digit_issues = []
            
            self.progress.emit("Memulai perbandingan data...")
            total_rows = len(gdf)
            
            # --- LANGKAH 3 & 4: Iterasi, Normalisasi, dan Bandingkan ---
            for index, feature in gdf.iterrows():
                # Update progress
                if index % 100 == 0:
                    progress_percent = int((index / total_rows) * 100)
                    self.progress.emit(f"Memproses data: {progress_percent}% ({index}/{total_rows})")
                
                gpkg_idsubsls = feature.get('idsubsls')
                if not gpkg_idsubsls or pd.isna(gpkg_idsubsls):
                    continue
                
                gpkg_idsubsls = str(gpkg_idsubsls).strip()
                gpkg_idsubsls_set.add(gpkg_idsubsls)
                
                gpkg_nmsls = str(feature.get('nmsls', '') or '').strip()
                gpkg_kdsubsls_raw = feature.get('kdsubsls', '')
                
                # Handle NaN values and normalize kdsubsls from GPKG
                if pd.isna(gpkg_kdsubsls_raw) or gpkg_kdsubsls_raw == '':
                    gpkg_kdsubsls_normalized = '00'
                    gpkg_kdsubsls_original = ''
                else:
                    gpkg_kdsubsls_str = str(gpkg_kdsubsls_raw).strip()
                    gpkg_kdsubsls_original = gpkg_kdsubsls_str
                    
                    # Cek digit kdsubsls di GPKG
                    if gpkg_kdsubsls_str != '' and gpkg_kdsubsls_str != '00':
                        if len(gpkg_kdsubsls_str) > 2:
                            gpkg_digit_issues.append(f"GPKG - ID: {gpkg_idsubsls}: kdsubsls '{gpkg_kdsubsls_str}' lebih dari 2 digit")
                        elif len(gpkg_kdsubsls_str) == 1:
                            gpkg_digit_issues.append(f"GPKG - ID: {gpkg_idsubsls}: kdsubsls '{gpkg_kdsubsls_str}' hanya 1 digit")
                    
                    # Normalisasi kdsubsls GPKG
                    if gpkg_kdsubsls_str == '0':
                        gpkg_kdsubsls_normalized = '00'
                    elif len(gpkg_kdsubsls_str) == 1 and gpkg_kdsubsls_str != '0':
                        gpkg_kdsubsls_normalized = f"0{gpkg_kdsubsls_str}"
                    else:
                        gpkg_kdsubsls_normalized = gpkg_kdsubsls_str

                result_row = {
                    'IDSUB_SLS': gpkg_idsubsls,
                    'NMSLS_GPKG': gpkg_nmsls,
                    'NMSLS_MASTER': '',
                    'KDSUBSLS_GPKG': gpkg_kdsubsls_normalized,  # Gunakan yang sudah dinormalisasi
                    'KDSUBSLS_GPKG_ORIGINAL': gpkg_kdsubsls_original,  # Simpan original untuk referensi
                    'KDSUBSLS_MASTER': '',
                    'KDSUBSLS_MASTER_ORIGINAL': '',
                    'Status': ''
                }

                if gpkg_idsubsls in master_data:
                    master_record = master_data[gpkg_idsubsls]
                    result_row['NMSLS_MASTER'] = master_record['nmsls']
                    result_row['KDSUBSLS_MASTER'] = master_record['kdsubsls']  # Yang sudah dinormalisasi
                    result_row['KDSUBSLS_MASTER_ORIGINAL'] = master_record['kdsubsls_original']  # Original dari Excel
                    
                    issues = []
                    if gpkg_nmsls != master_record['nmsls']:
                        issues.append('Beda NMSLS')
                    
                    # Bandingkan yang sudah dinormalisasi
                    if gpkg_kdsubsls_normalized != master_record['kdsubsls']:
                        issues.append('Beda KdSubSLS')
                    
                    result_row['Status'] = ', '.join(issues) if issues else 'Sesuai'
                else:
                    result_row['Status'] = 'Tidak Ditemukan di Master'
                
                all_results.append(result_row)
            
            # --- LANGKAH 5: Cek data yang hilang di GPKG ---
            self.progress.emit("Mengecek data yang hilang di GeoPackage...")
            master_idsubsls_set = set(master_data.keys())
            missing_in_gpkg = master_idsubsls_set - gpkg_idsubsls_set
            
            for idsubsls in missing_in_gpkg:
                master_record = master_data[idsubsls]
                all_results.append({
                    'IDSUB_SLS': idsubsls,
                    'NMSLS_GPKG': '',
                    'NMSLS_MASTER': master_record['nmsls'],
                    'KDSUBSLS_GPKG': '',
                    'KDSUBSLS_GPKG_ORIGINAL': '',
                    'KDSUBSLS_MASTER': master_record['kdsubsls'],
                    'KDSUBSLS_MASTER_ORIGINAL': master_record['kdsubsls_original'],
                    'Status': 'Tidak Ditemukan di GeoPackage'
                })

            # Simpan informasi digit issues ke results
            digit_issues_result = {
                'IDSUB_SLS': '=== ISSUE DIGIT KDSUBSLS ===',
                'NMSLS_GPKG': '',
                'NMSLS_MASTER': '',
                'KDSUBSLS_GPKG': f"Total issue GPKG: {len(gpkg_digit_issues)}",
                'KDSUBSLS_GPKG_ORIGINAL': '',
                'KDSUBSLS_MASTER': f"Total issue Excel: {len(master_digit_issues)}",
                'KDSUBSLS_MASTER_ORIGINAL': '',
                'Status': 'Laporan Digit'
            }
            all_results.append(digit_issues_result)
            
            # Tambahkan detail issues ke results
            for issue in gpkg_digit_issues[:10]:  # Batasi agar tidak terlalu panjang
                all_results.append({
                    'IDSUB_SLS': issue.split(':')[1].strip(),
                    'NMSLS_GPKG': '',
                    'NMSLS_MASTER': '',
                    'KDSUBSLS_GPKG': issue,
                    'KDSUBSLS_GPKG_ORIGINAL': '',
                    'KDSUBSLS_MASTER': '',
                    'KDSUBSLS_MASTER_ORIGINAL': '',
                    'Status': 'Issue Digit GPKG'
                })
            
            for issue in master_digit_issues[:10]:  # Batasi agar tidak terlalu panjang
                all_results.append({
                    'IDSUB_SLS': issue.split(':')[1].strip(),
                    'NMSLS_GPKG': '',
                    'NMSLS_MASTER': '',
                    'KDSUBSLS_GPKG': '',
                    'KDSUBSLS_GPKG_ORIGINAL': '',
                    'KDSUBSLS_MASTER': issue,
                    'KDSUBSLS_MASTER_ORIGINAL': '',
                    'Status': 'Issue Digit Excel'
                })

            self.finished.emit(all_results)

        except FileNotFoundError as e:
            self.error.emit(f"File tidak ditemukan: {e}")
        except PermissionError as e:
            self.error.emit(f"Tidak ada izin untuk membaca file: {e}")
        except pd.errors.EmptyDataError:
            self.error.emit("File Excel kosong")
        except KeyError as e:
            self.error.emit(f"Error: Kolom tidak ditemukan. Pastikan file Anda memiliki kolom 'idsubsls', 'nmsls', dan 'kdsubsls'. Detail: {e}")
        except Exception as e:
            self.error.emit(f"Terjadi error: {str(e)}")


# --- Main Application Window ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Aplikasi Pengecek Konsistensi Data SLS")
        self.setGeometry(100, 100, 800, 600)
        
        self.results_data = []
        self.worker = None

        # Central Widget and Layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # File Selection Layout
        file_layout = QVBoxLayout()
        
        # GeoPackage file
        gpkg_layout = QHBoxLayout()
        self.gpkg_label = QLabel("File GeoPackage (.gpkg):")
        self.gpkg_path_edit = QLineEdit()
        self.gpkg_browse_btn = QPushButton("Pilih File")
        self.gpkg_browse_btn.clicked.connect(self.browse_gpkg)
        gpkg_layout.addWidget(self.gpkg_label)
        gpkg_layout.addWidget(self.gpkg_path_edit)
        gpkg_layout.addWidget(self.gpkg_browse_btn)
        
        # Excel file
        excel_layout = QHBoxLayout()
        self.excel_label = QLabel("File Master Excel (.xlsx):")
        self.excel_path_edit = QLineEdit()
        self.excel_browse_btn = QPushButton("Pilih File")
        self.excel_browse_btn.clicked.connect(self.browse_excel)
        excel_layout.addWidget(self.excel_label)
        excel_layout.addWidget(self.excel_path_edit)
        excel_layout.addWidget(self.excel_browse_btn)

        file_layout.addLayout(gpkg_layout)
        file_layout.addLayout(excel_layout)

        # Action Buttons Layout
        action_layout = QHBoxLayout()
        self.run_check_btn = QPushButton("Mulai Pengecekan")
        self.run_check_btn.clicked.connect(self.run_check)
        self.export_csv_btn = QPushButton("Ekspor Hasil ke CSV")
        self.export_csv_btn.clicked.connect(self.export_csv)
        self.export_csv_btn.setEnabled(False)
        self.cancel_btn = QPushButton("Batalkan")
        self.cancel_btn.clicked.connect(self.cancel_check)
        self.cancel_btn.setEnabled(False)
        
        action_layout.addWidget(self.run_check_btn)
        action_layout.addWidget(self.export_csv_btn)
        action_layout.addWidget(self.cancel_btn)

        # Log/Results Display
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setFontFamily("Courier New")

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)

        # Add all widgets to main layout
        main_layout.addLayout(file_layout)
        main_layout.addLayout(action_layout)
        main_layout.addWidget(QLabel("Log Hasil Pengecekan:"))
        main_layout.addWidget(self.results_text)
        main_layout.addWidget(self.progress_bar)

    def browse_gpkg(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Pilih File GeoPackage", "", "GeoPackage Files (*.gpkg)")
        if file_path:
            self.gpkg_path_edit.setText(file_path)

    def browse_excel(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Pilih File Excel", "", "Excel Files (*.xlsx)")
        if file_path:
            self.excel_path_edit.setText(file_path)

    def validate_files(self, gpkg_path, excel_path):
        """Validasi keberadaan dan format file"""
        if not os.path.exists(gpkg_path):
            return False, "File GeoPackage tidak ditemukan"
        if not os.path.exists(excel_path):
            return False, "File Excel tidak ditemukan"
        if not gpkg_path.lower().endswith('.gpkg'):
            return False, "File GeoPackage harus berekstensi .gpkg"
        if not excel_path.lower().endswith(('.xlsx', '.xls')):
            return False, "File Excel harus berekstensi .xlsx atau .xls"
        return True, "OK"

    def run_check(self):
        gpkg_path = self.gpkg_path_edit.text()
        excel_path = self.excel_path_edit.text()

        if not gpkg_path or not excel_path:
            QMessageBox.warning(self, "Input Tidak Lengkap", "Silakan pilih kedua file (GeoPackage dan Excel) terlebih dahulu.")
            return
        
        # Validasi file
        is_valid, message = self.validate_files(gpkg_path, excel_path)
        if not is_valid:
            QMessageBox.warning(self, "File Tidak Valid", message)
            return
        
        # Reset UI
        self.set_ui_enabled(False)
        self.results_text.clear()
        self.results_data = []
        self.export_csv_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0) # Indeterminate progress

        # Start worker thread
        self.worker = CheckWorker(gpkg_path, excel_path)
        self.worker.progress.connect(self.update_log)
        self.worker.finished.connect(self.on_check_finished)
        self.worker.error.connect(self.on_check_error)
        self.worker.start()

    def update_log(self, message):
        self.results_text.append(message)

    def on_check_finished(self, results):
        self.results_data = results
        self.update_log("\n--- Pengecekan Selesai ---")

        # Filter hanya hasil pengecekan data (bukan laporan digit)
        data_results = [r for r in results if 'ISSUE DIGIT' not in r['IDSUB_SLS'] and 'Issue Digit' not in r['Status']]
        mismatches = [row for row in data_results if row['Status'] != 'Sesuai']

        # Hitung statistik
        total = len(data_results)
        sesuai = len([r for r in data_results if r['Status'] == 'Sesuai'])
        tidak_ditemukan_master = len([r for r in data_results if r['Status'] == 'Tidak Ditemukan di Master'])
        tidak_ditemukan_gpkg = len([r for r in data_results if r['Status'] == 'Tidak Ditemukan di GeoPackage'])
        beda_nmsls = len([r for r in data_results if 'Beda NMSLS' in r['Status']])
        beda_kdsubsls = len([r for r in data_results if 'Beda KdSubSLS' in r['Status']])

        # Tampilkan summary
        self.results_text.append(f"\n=== SUMMARY HASIL ===")
        self.results_text.append(f"Total Data: {total}")
        self.results_text.append(f"Data Sesuai: {sesuai}")
        self.results_text.append(f"Tidak Ditemukan di Master: {tidak_ditemukan_master}")
        self.results_text.append(f"Tidak Ditemukan di GeoPackage: {tidak_ditemukan_gpkg}")
        self.results_text.append(f"Beda NMSLS: {beda_nmsls}")
        self.results_text.append(f"Beda KdSubSLS: {beda_kdsubsls}")

        if not mismatches:
            self.results_text.append("\nSELAMAT! Semua data konsisten.")
        else:
            self.results_text.append(f"\nDitemukan {len(mismatches)} ketidakcocokan data:")
            # Tampilkan beberapa contoh di log
            for i, mismatch in enumerate(mismatches[:20], 1):
                 self.results_text.append(f"{i}. IDSUB_SLS {mismatch['IDSUB_SLS']}: {mismatch['Status']}")
            if len(mismatches) > 20:
                self.results_text.append("...")
                self.results_text.append("\n(Hasil selengkapnya dapat diekspor ke CSV)")

        self.export_csv_btn.setEnabled(True)
        self.set_ui_enabled(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)

    def on_check_error(self, error_message):
        self.set_ui_enabled(True)
        self.progress_bar.setVisible(False)
        QMessageBox.critical(self, "Error", error_message)
        self.update_log(f"ERROR: {error_message}")

    def export_csv(self):
        if not self.results_data:
            QMessageBox.information(self, "Tidak Ada Data", "Tidak ada data hasil pengecekan untuk diekspor.")
            return

        file_path, _ = QFileDialog.getSaveFileName(self, "Simpan Hasil CSV", "", "CSV Files (*.csv)")
        if file_path:
            try:
                df = pd.DataFrame(self.results_data)
                # Pastikan kdsubsls ditampilkan dengan format yang benar
                if 'KDSUBSLS_GPKG' in df.columns:
                    df['KDSUBSLS_GPKG'] = df['KDSUBSLS_GPKG'].apply(lambda x: f"{x:0>2}" if str(x).isdigit() and len(str(x)) == 1 else str(x))
                if 'KDSUBSLS_MASTER' in df.columns:
                    df['KDSUBSLS_MASTER'] = df['KDSUBSLS_MASTER'].apply(lambda x: f"{x:0>2}" if str(x).isdigit() and len(str(x)) == 1 else str(x))
                
                df.to_csv(file_path, index=False)
                QMessageBox.information(self, "Ekspor Berhasil", f"Hasil berhasil disimpan ke:\n{file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Gagal Menyimpan", f"Gagal menyimpan file CSV. Error: {e}")

    def cancel_check(self):
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()
            self.update_log("Proses dibatalkan oleh pengguna")
            self.set_ui_enabled(True)
            self.progress_bar.setVisible(False)

    def set_ui_enabled(self, enabled):
        self.gpkg_browse_btn.setEnabled(enabled)
        self.excel_browse_btn.setEnabled(enabled)
        self.run_check_btn.setEnabled(enabled)
        self.cancel_btn.setEnabled(not enabled)  # Tombol batal aktif saat proses berjalan
        if not enabled:
            self.export_csv_btn.setEnabled(False)

    def closeEvent(self, event):
        """Konfirmasi sebelum menutup aplikasi"""
        if self.worker and self.worker.isRunning():
            reply = QMessageBox.question(
                self, 'Konfirmasi',
                'Proses masih berjalan. Yakin ingin menutup?',
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.worker.terminate()
                self.worker.wait()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())