import nibabel as nib
import numpy as np
import pandas as pd
import os
import shap
import model
from data_util import CNN_Data, split_csv, brain_regions
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from explain_pipeline import *

def test_task1():
    df=pd.read_csv('../output/task-1.csv')
    assert df['value'][0]==18 and df['value'][1]==1
