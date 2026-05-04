import argparse
import os
import torch
import pandas as pd
from PIL import Image
from tqdm import tqdm
from transformers import AutoImageProcessor, AutoModel


def parse_args():
    parser = argparse.ArgumentParser(description="Extract visual features using Swin Transformer")
    parser.add_argument("--data_dir", type=str, default=r"E:\AIO\Project\MM-ShortVideo-Rec\data\microlens-5k",
                        help="Path to dataset directory (microlens-5k)")
    return parser.parse_args()


MODEL_NAME = "microsoft/swin-tiny-patch4-window7-224"
VISUAL_DIM = 768

def extract(model, processor, img_path: str, device: str) -> torch.Tensor:
    try:
        image = Image.open(img_path).convert("RGB")
        inputs = {k: v.to(device) for k, v in processor(images=image, return_tensors="pt").items()}
        return model(**inputs).pooler_output.squeeze(0).cpu()
    except Exception:
        return torch.zeros(VISUAL_DIM)

def main():
    args = parse_args()
    pairs_csv   = os.path.join(args.data_dir, "pairs.csv")
    covers_dir  = os.path.join(args.data_dir, "covers")
    output_path = os.path.join(args.data_dir, "visual_embeddings.pt")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Device: {device}")

    unique_items = pd.read_csv(pairs_csv)['item'].unique()
    print(f"Items to process: {len(unique_items)}")

    processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME).to(device).eval()

    visual_embeddings = {}
    with torch.no_grad():
        for item_id in tqdm(unique_items, desc="Extracting"):
            img_path = os.path.join(covers_dir, f"{item_id}.jpg")
            visual_embeddings[item_id] = extract(model, processor, img_path, device)

    torch.save(visual_embeddings, output_path)
    print(f"Saved {len(visual_embeddings)} embeddings → {output_path}")

if __name__ == "__main__":
    main()
