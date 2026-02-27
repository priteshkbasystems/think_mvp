def run_env_check():
    print("Checking environment...")

    import torch
    import transformers
    import pandas as pd

    print("Torch version:", torch.__version__)
    print("Transformers version:", transformers.__version__)
    print("Pandas version:", pd.__version__)

    print("CUDA available:", torch.cuda.is_available())

    print("Environment OK ✅")