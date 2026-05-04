import os
import zipfile
import gdown
import kagglehub
import shutil

def download_and_extract():
    # Xác định thư mục gốc của project (thư mục cha của 'utils')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    # Thư mục đích tuyệt đối: project_root/data/microlens-5k
    target_dir = os.path.join(project_root, "data", "microlens-5k")
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(target_dir, exist_ok=True)
    
    # 1. Tải toàn bộ thư mục từ Drive về
    print(f"Đang tải dữ liệu từ Google Drive về '{target_dir}'...")
    url = "https://drive.google.com/drive/folders/156RLMeYhBh_0uE3JpLNpP9RIpvkOWqt1?usp=sharing"
    
    # Sử dụng gdown để tải thư mục
    # output=target_dir sẽ tải nội dung vào thư mục này
    gdown.download_folder(url, output=target_dir, quiet=False, use_cookies=False)
    
    # 2. Giải nén covers.zip ngay tại folder data/microlens-5k
    zip_path = os.path.join(target_dir, "covers.zip")
    if os.path.exists(zip_path):
        print(f"\nĐang giải nén {zip_path}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
        print("Giải nén hoàn tất.")
    else:
        print(f"\nKhông tìm thấy file {zip_path} để giải nén.")

    # 3. Tải dataset từ Kaggle
    kaggle_target_folder = os.path.join(target_dir, "MicroLens-100k_videos_Part_1")
    if not os.path.exists(kaggle_target_folder):
        print(f"\nĐang tải dataset từ Kaggle (shuike520/microlens-100k-videos-part-1)...")
        try:
            kaggle_path = kagglehub.dataset_download("shuike520/microlens-100k-videos-part-1")
            print(f"Dữ liệu Kaggle đã được tải về tại: {kaggle_path}")
            
            # Di chuyển dữ liệu vào thư mục data/microlens-5k nếu chưa có
            for item in os.listdir(kaggle_path):
                s = os.path.join(kaggle_path, item)
                d = os.path.join(target_dir, item)
                if not os.path.exists(d):
                    print(f"Đang di chuyển {item} sang {target_dir}...")
                    shutil.move(s, d)
            print("Đã hoàn tất sắp xếp dữ liệu Kaggle.")
        except Exception as e:
            print(f"Lỗi khi tải từ Kaggle: {e}")
    else:
        print(f"\nDữ liệu Kaggle đã tồn tại trong {kaggle_target_folder}, bỏ qua việc tải.")

    # 4. Kiểm tra lại cấu trúc thư mục local
    print(f"\nKiểm tra lại cấu trúc thư mục '{target_dir}':")
    if os.path.exists(target_dir):
        for root, dirs, files in os.walk(target_dir):
            level = root.replace(target_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            print(f"{indent}{os.path.basename(root)}/")
            
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                file_path = os.path.join(root, f)
                try:
                    size = os.path.getsize(file_path) / (1024 * 1024) # MB
                    print(f"{subindent}{f} ({size:.2f} MB)")
                except OSError:
                    print(f"{subindent}{f} (Không thể lấy kích thước)")
    else:
        print(f"Thư mục {target_dir} không tồn tại.")

if __name__ == "__main__":
    download_and_extract()
