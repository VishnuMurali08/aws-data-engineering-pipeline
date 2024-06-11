import transformers
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import os

# Load the pre-trained model and tokenizer
model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Save the model and tokenizer
model_path = "model/"
code_path = "code/"

if not os.path.exists(model_path):
    os.makedirs(model_path)
model.save_pretrained(model_path)
tokenizer.save_pretrained(model_path)

print("Model and tokenizer saved successfully.")
