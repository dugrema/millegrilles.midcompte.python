import sys
import os
import traceback

try:
    from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
except ImportError as e:
    print(f"Error importing: {e}")
    sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_idmg.py <cert_file_path>")
        sys.exit(1)

    cert_path = sys.argv[1]
    if not os.path.exists(cert_path):
        print(f"Error: File {cert_path} does not exist.")
        sys.exit(1)

    try:
        enveloppe = EnveloppeCertificat.from_file(cert_path)
        print(enveloppe.idmg)
    except Exception as e:
        print(f"Error processing certificate: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
