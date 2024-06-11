import tarfile
import os

# Directory where model, inference.py, and requirements.txt are located
base_dir = "C:\\Users\\ZZ03NR834\\Desktop\\fyp2024"

# Create tar.gz file
with tarfile.open(os.path.join(base_dir, "model.tar.gz"), "w:gz") as tar:
    tar.add(os.path.join(base_dir, "model"), arcname="model")
    tar.add(os.path.join(base_dir, "inference.py"), arcname="code/inference.py")
    tar.add(os.path.join(base_dir, "requirements.txt"), arcname="code/requirements.txt")

print("model.tar.gz created successfully.")