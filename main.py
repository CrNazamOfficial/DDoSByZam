
import json
import os
from urllib.parse import urlparse

CONFIG_FILE = "config.json"


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {"targets": []}

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data.get("targets"), list):
            data["targets"] = []

        return data

    except (json.JSONDecodeError, OSError):
        print("[!] config.json tidak bisa dibaca.")
        return {"targets": []}


def save_config(data):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        return True
    except OSError as error:
        print(f"[!] Gagal menyimpan: {error}")
        return False


def normalize_domain(value):
    value = value.strip()

    if not value:
        return None

    if "://" not in value:
        value = "https://" + value

    parsed = urlparse(value)

    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        return None

    return parsed.hostname.lower()


def add_domain(data):
    domain = normalize_domain(input("\nMasukkan domain: "))

    if not domain:
        print("[!] Format domain tidak valid.")
        return

    if domain in data["targets"]:
        print("[!] Domain sudah ada.")
        return

    data["targets"].append(domain)

    if save_config(data):
        print(f"[+] Domain tersimpan: {domain}")


def view_domains(data):
    print("\n===== DAFTAR DOMAIN =====")

    if not data["targets"]:
        print("Belum ada domain tersimpan.")
        return

    for index, domain in enumerate(data["targets"], start=1):
        print(f"{index}. {domain}")


def remove_domain(data):
    view_domains(data)

    if not data["targets"]:
        return

    choice = input("\nNomor domain yang dihapus: ").strip()

    if not choice.isdigit():
        print("[!] Masukkan nomor yang benar.")
        return

    index = int(choice) - 1

    if index < 0 or index >= len(data["targets"]):
        print("[!] Nomor tidak ditemukan.")
        return

    removed = data["targets"].pop(index)

    if save_config(data):
        print(f"[+] Domain dihapus: {removed}")


def main():
    while True:
        data = load_config()

        print("""
================================
          DDOSBYZAM
================================
[1] Tambah Domain
[2] Lihat Domain
[3] Hapus Domain
[4] Keluar
================================
""")

        choice = input("Pilih menu: ").strip()

        if choice == "1":
            add_domain(data)
        elif choice == "2":
            view_domains(data)
        elif choice == "3":
            remove_domain(data)
        elif choice == "4":
            print("Keluar.")
            break
        else:
            print("[!] Pilihan tidak valid.")


if __name__ == "__main__":
    main()
