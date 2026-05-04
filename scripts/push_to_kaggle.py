import os
import json
from kaggle.api.kaggle_api_extended import KaggleApi
os.environ['KAGGLE_API_TOKEN'] = "KGAT_d6f2c32f5bb2a7f0d391d65c20a782c6"


def push_to_kaggle(folder_path, dataset_id, dataset_title, is_public=False, update_notes="Cập nhật version mới"):
    """
    Hàm tự động push một thư mục local lên Kaggle Dataset.
    """
    # 1. Tạo file dataset-metadata.json động bằng code
    metadata = {
        "title": dataset_title,
        "id": dataset_id,
        "licenses": [
            {
                "name": "CC0-1.0"
            }
        ]
    }
    
    metadata_path = os.path.join(folder_path, 'dataset-metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
        
    print(f"Đã tạo metadata tại: {metadata_path}")

    # 2. Khởi tạo và xác thực API
    api = KaggleApi()
    api.authenticate() # Sẽ tự động tìm file ~/.kaggle/kaggle.json

    # 3. Push lên Kaggle
    try:
        print(f"Đang thử tạo Dataset mới: {dataset_id}...")
        api.dataset_create_new(
            folder=folder_path,
            public=is_public,
            dir_mode='skip' # Bỏ qua các file ẩn
        )
        print("🎉 Tạo dataset mới thành công!")
        
    except Exception as e:
        # Nếu dataset đã tồn tại, API sẽ quăng lỗi. Lúc này ta chuyển sang tạo version mới.
        print(f"Dataset có thể đã tồn tại. Chuyển sang chế độ cập nhật (Update Version)...")
        print(f"Chi tiết lỗi: {e}")
        
        try:
            api.dataset_create_version(
                folder=folder_path,
                version_notes=update_notes,
                dir_mode='skip'
            )
            print("✅ Cập nhật version mới thành công!")
        except Exception as update_err:
            print(f"❌ Lỗi khi cập nhật dataset: {update_err}")

# ==========================================
# CÁCH SỬ DỤNG
# ==========================================
if __name__ == "__main__":
    # Thay đổi các thông số dưới đây cho phù hợp với bạn
    FOLDER_CHUA_DATA = "/home/minh/workspaces/MM-ShortVideo-Rec/data/microlens-5k/videos" 
    KAGGLE_USERNAME = "tsunmm"  # TODO: THAY BẰNG USERNAME KAGGLE CỦA BẠN
    TEN_DATASET = "microlens-5k-videos"    # Tên ID (chỉ viết thường, không dấu, dùng gạch ngang)
    
    if not os.path.exists(FOLDER_CHUA_DATA):
        print(f"❌ Thư mục {FOLDER_CHUA_DATA} không tồn tại. Vui lòng kiểm tra lại đường dẫn!")
    else:
        # Gọi hàm
        push_to_kaggle(
            folder_path=FOLDER_CHUA_DATA,
            dataset_id=f"{KAGGLE_USERNAME}/{TEN_DATASET}",
            dataset_title="Microlens 5K Videos Dataset",
            is_public=False, # Để False (Private) để test cho an toàn, nếu muốn công khai thì đổi thành True
            update_notes="Tự động push video data bằng Python script"
        )
