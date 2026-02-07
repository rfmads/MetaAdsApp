# scripts/test_refresh_status.py
from config.config import META_USER_TOKEN
from services.status_refresh_service import refresh_all_real_status

def main():
    # لا يحتاج توكن، بس نخليه مثل باقي التستات
    refresh_all_real_status()

if __name__ == "__main__":
    main()
