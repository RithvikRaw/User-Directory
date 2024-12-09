import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.tasks import get_training_data

context = {}
training_data = get_training_data(context)
training_data.to_csv("model/input/training_data.csv", index=False)