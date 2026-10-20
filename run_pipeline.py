"""Run the complete offline ML pipeline: cleaning -> training."""
from data_processing.clean_data import clean_data
from models.train_model import train
if __name__=="__main__":
    clean_data(); train(); print("\nPipeline complete. Start API/dashboard with the commands in README.md.")
