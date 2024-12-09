import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.tasks import get_prediction_data

context = {}

prediction_data, all_data = get_prediction_data(context)
prediction_data.to_csv("model/input/prediction_data.csv", index=False)
all_data.to_csv("model/input/all_user_data.csv", index=False)