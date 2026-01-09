import os
import shutil
import hashlib
import unittest
import tempfile
import random
import string
import sys
import logging
from pathlib import Path
from threading import Lock
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

def file_hash(path):
    hasher = hashlib.md5()
    with open(path, 'rb') as afile:
        while chunk := afile.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def should_copy(src_file, dst_file):
    """Determina se un file deve essere copiato confrontando esistenza, timestamp e hash."""
    if not os.path.exists(dst_file):
        return True
    if os.path.getmtime(src_file) > os.path.getmtime(dst_file):
        return file_hash(src_file) != file_hash(dst_file)
    return False

    return False

def copy_file(src_file, dst_file):
    """Copia un file dalla sorgente alla destinazione"""
    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
    shutil.copy2(src_file, dst_file)

def delete_file(path):
    """Elimina un file"""
    if os.path.exists(path):
        os.remove(path)

def should_skip_file(filename):
    """Determina se un file deve essere ignorato (file di sistema)"""
    skip_files = {
        '.DS_Store',      # macOS
        '._.DS_Store',    # macOS (resource fork)
        'Thumbs.db',      # Windows
        'Desktop.ini',    # Windows
        '.localized',     # macOS
        '__MACOSX',       # macOS (cartella ZIP)
    }
    
    if filename.startswith('._'):
        return True
        
    return filename in skip_files

def scan_files(base_dir: str, apply_skip_filter=True):
    """Scansiona ricorsivamente una directory e restituisce tutti i file"""
    base_path = Path(base_dir)
    file_list = []
    
    for root, _, files in os.walk(base_path):
        for f in files:
            if apply_skip_filter and should_skip_file(f):
                continue
                
            abs_path = Path(root) / f
            rel_path = abs_path.relative_to(base_path)
            file_list.append((str(abs_path), str(rel_path)))
    
    return file_list

def scan_directories(base_dir: str):
    """Scansiona ricorsivamente una directory e restituisce tutte le cartelle"""
    base_path = Path(base_dir)
    dir_list = []
    
    for root, dirs, _ in os.walk(base_path):
        for d in dirs:
            abs_path = Path(root) / d
            rel_path = abs_path.relative_to(base_path)
            dir_list.append((str(abs_path), str(rel_path)))
    
    return dir_list

def sync_directories(source_folder, destination_folder):
    """Sincronizza le directory vuote"""
    src_dirs = scan_directories(source_folder)
    
    created_count = 0
    for _, rel_path in src_dirs:
        dst_dir = os.path.join(destination_folder, rel_path)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir, exist_ok=True)
            logging.info(f"Creata directory: {rel_path}")
            created_count += 1

    if created_count == 0:
        logging.info("Nessuna nuova directory da creare")


def remove_obsolete_directories(source_folder, destination_folder):
    """Rimuove le directory vuote presenti nella destinazione ma non nella sorgente."""
    src_dirs = set(rel for _, rel in scan_directories(source_folder))
    dst_dirs = scan_directories(destination_folder)
    
    dst_dirs.sort(key=lambda x: x[1].count(os.sep), reverse=True)
    
    removed_count = 0
    for abs_dst, rel_path in dst_dirs:
        if rel_path not in src_dirs:
            try:
                if not os.listdir(abs_dst):
                    os.rmdir(abs_dst)
                    logging.info(f"Rimossa directory obsoleta: {rel_path}")
                    removed_count += 1
                else:
                    logging.warning(f"Directory non vuota, non rimossa: {rel_path}")
            except Exception as e:
                logging.error(f"Errore nella rimozione della directory {abs_dst}: {e}")
    
    if removed_count > 0:
        logging.info(f"Rimosse {removed_count} directory obsolete")
    else:
        logging.info("Nessuna directory obsoleta da rimuovere")

def sync_worker(args):
    """Sincronizza un gruppo di file copiando solo quelli modificati usando un pool di thread."""
    chunk, destination_folder, threads_per_worker = args

    def thread_copy(src_file, rel_path):
        dst_file = os.path.join(destination_folder, rel_path)
        if should_copy(src_file, dst_file):
            try:
                copy_file(src_file, dst_file)
                logging.debug(f"Copiato: {rel_path}")
            except Exception as e:
                logging.error(f"Errore nella copia di {src_file}: {e}")

    with ThreadPoolExecutor(max_workers=threads_per_worker) as executor:
        for src_file, rel_path in chunk:
            executor.submit(thread_copy, src_file, rel_path)

def copy_worker(args):
    """Copia un gruppo di file usando un pool di thread per parallelizzare le operazioni."""
    chunk, destination_folder, threads_per_worker = args

    def thread_copy(src_file, rel_path):
        dst_file = os.path.join(destination_folder, rel_path)
        try:
            copy_file(src_file, dst_file)
            logging.debug(f"Copiato: {rel_path}")
        except Exception as e:
            logging.error(f"Errore nella copia di {src_file}: {e}")

    with ThreadPoolExecutor(max_workers=threads_per_worker) as executor:
        for src_file, rel_path in chunk:
            executor.submit(thread_copy, src_file, rel_path)

def delete_file_safe(path):
    """Elimina un file in modo sicuro gestendo eventuali errori con logging."""
    try:
        delete_file(path)
        logging.info(f"Rimosso: {os.path.relpath(path)}")
    except Exception as e:
        logging.error(f"Errore nell'eliminazione di {path}: {e}")


def remove_obsolete_files(source_folder, destination_folder):
    """Rimuove i file nella destinazione che non esistono più nella sorgente o sono file di sistema."""
    src_files = set(rel for _, rel in scan_files(source_folder, apply_skip_filter=True))
    dst_files = scan_files(destination_folder, apply_skip_filter=False)
    
    removed_count = 0
    for abs_dst, rel_path in dst_files:
        filename = os.path.basename(rel_path)
        if should_skip_file(filename):
            try:
                delete_file(abs_dst)
                logging.info(f"Rimosso file di sistema: {rel_path}")
                removed_count += 1
            except Exception as e:
                logging.error(f"Errore nella rimozione del file di sistema {abs_dst}: {e}")
        elif rel_path not in src_files:
            try:
                delete_file(abs_dst)
                logging.info(f"Rimosso file obsoleto: {rel_path}")
                removed_count += 1
            except Exception as e:
                logging.error(f"Errore nella rimozione di {abs_dst}: {e}")
    
    if removed_count > 0:
        logging.info(f"Rimossi {removed_count} file obsoleti")
    else:
        logging.info("Nessun file obsoleto da rimuovere")

def sync_folders(source_folder, destination_folder, threads_per_worker=4):
    """Sincronizza due cartelle usando multiprocessing e multithreading per copiare, aggiornare e rimuovere file."""
    if not os.path.exists(source_folder):
        raise ValueError(f"La cartella sorgente {source_folder} non esiste")

    logging.info(f"Sincronizzazione da {source_folder} a {destination_folder}")
    os.makedirs(destination_folder, exist_ok=True)
    sync_directories(source_folder, destination_folder)

    src_files = scan_files(source_folder, apply_skip_filter=True)
    copy_tasks = []

    for abs_src, rel_path in src_files:
        abs_dst = os.path.join(destination_folder, rel_path)
        if should_copy(abs_src, abs_dst):
            copy_tasks.append((abs_src, rel_path))

    logging.info(f"Trovati {len(copy_tasks)} file da copiare/aggiornare")

    src_rel_paths = set(rel for _, rel in src_files)
    dst_files = scan_files(destination_folder, apply_skip_filter=False)

    delete_tasks = []
    for abs_dst, rel_path in dst_files:
        filename = os.path.basename(rel_path)
        if should_skip_file(filename) or rel_path not in src_rel_paths:
            delete_tasks.append(abs_dst)

    logging.info(f"Trovati {len(delete_tasks)} file da rimuovere")

    if copy_tasks:
        num_processes = min(cpu_count(), max(1, len(copy_tasks) // 100))
        chunk_size = len(copy_tasks) // num_processes + 1
        chunks = [copy_tasks[i:i + chunk_size] for i in range(0, len(copy_tasks), chunk_size)]
        args = [(chunk, destination_folder, threads_per_worker) for chunk in chunks]

        with Pool(processes=num_processes) as pool:
            pool.map(copy_worker, args)

        logging.info("Sincronizzazione file completata")
    else:
        logging.info("Nessun file da copiare")

    if delete_tasks:
        with ThreadPoolExecutor(max_workers=threads_per_worker) as executor:
            for path in delete_tasks:
                executor.submit(delete_file_safe, path)
        logging.info("Eliminazione file completata")
    else:
        logging.info("Nessun file da eliminare")

    logging.info("Controllo directory obsolete...")
    remove_obsolete_directories(source_folder, destination_folder)



class TestRealScenarios(unittest.TestCase):
    """Test di sincronizzazione in scenari reali: pochi file, stress test e strutture annidate complesse."""
    def setUp(self):
        self.src_dir = tempfile.mkdtemp()
        self.dst_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.src_dir)
        shutil.rmtree(self.dst_dir)

    def create_file(self, folder, relative_path, content="test"):
        path = os.path.join(folder, relative_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        return path

    def test_simple_sync_few_files(self):
        for i in range(5):
            self.create_file(self.src_dir, f"file_{i}.txt", f"content {i}")
        sync_folders(self.src_dir, self.dst_dir)
        for i in range(5):
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, f"file_{i}.txt")))

    def test_stress_many_files(self):
        for i in range(3000):
            name = f"file_{i}.txt"
            content = ''.join(random.choices(string.ascii_letters, k=100))
            self.create_file(self.src_dir, name, content)
        sync_folders(self.src_dir, self.dst_dir)
        for i in range(3000):
            self.assertTrue(os.path.exists(os.path.join(self.dst_dir, f"file_{i}.txt")))

    def test_complex_nested_structure(self):
        for i in range(10):
            for j in range(10):
                for k in range(2):
                    rel_path = f"folder_{i}/subfolder_{j}/file_{k}.txt"
                    self.create_file(self.src_dir, rel_path, f"{i}-{j}-{k}")
        sync_folders(self.src_dir, self.dst_dir)
        for i in range(10):
            for j in range(10):
                for k in range(2):
                    self.assertTrue(os.path.exists(os.path.join(self.dst_dir, f"folder_{i}/subfolder_{j}/file_{k}.txt")))


def sync_my_folders():
    """Sincronizza le cartelle configurate con output informativo per l'utente."""
    source_folder = "/Users/andrea/Desktop/Cartella_principale" #Inserire percorso cartella principale
    destination_folder = "/Users/andrea/Desktop/Cartella_di_destinazione" #Inserire percorso cartella di destinazione

    print(f"📁 Cartella sorgente: {source_folder}")
    print(f"📁 Cartella destinazione: {destination_folder}")

    if not os.path.exists(source_folder):
        print(f"❌ Cartella sorgente non trovata: {source_folder}")
        return

    if not os.path.exists(destination_folder):
        print(f"📂 Cartella destinazione non esiste, la creo...")
        os.makedirs(destination_folder)

    print("🔄 Avvio sincronizzazione...")
    sync_folders(source_folder, destination_folder)
    print("✅ Sincronizzazione completata.")

#if __name__ == "__main__":
    """Esegue la sincronizzazione delle cartelle o UNITTEST."""

    # unittest.main() 
    
    #sync_my_folders()

if __name__ == "__main__":
    import sys
    if getattr(sys, 'frozen', False):  # se è eseguito da .app
        sync_my_folders()
    else:
        # Se vuoi lanciare test da terminale:
        # unittest.main()
        sync_my_folders()

