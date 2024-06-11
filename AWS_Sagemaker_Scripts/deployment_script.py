import boto3
import botocore
import sagemaker
from sagemaker.pytorch import PyTorchModel
from sagemaker.huggingface import HuggingFaceModel
from sagemaker import get_execution_role
import time

endpoint_name = "bert-sentiment-endpoint" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
model_s3_path ='s3://combine-scripts/sagemaker/train-deploy/model.tar.gz'
role = get_execution_role()

model = HuggingFaceModel(
    entry_point="inference.py",  # Make sure this points correctly to your inference script
    source_dir=model_s3_path,  # This points to your code directory containing the inference script
    model_data=model_s3_path,
    role=role,
    transformers_version="4.10.2",
    pytorch_version="1.8.1",
    py_version="py36"
)

predictor = model.deploy(
    initial_instance_count=1, instance_type="ml.m5.2xlarge", endpoint_name=endpoint_name
)
print(f"Model deployed. Endpoint name: {predictor.endpoint_name}")