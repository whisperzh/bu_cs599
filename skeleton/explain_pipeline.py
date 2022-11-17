import argparse
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
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

# This is a color map that you can use to plot the SHAP heatmap on the input MRI
colors = []
for l in np.linspace(1, 0, 100):
    colors.append((30. / 255, 136. / 255, 229. / 255, l))
for l in np.linspace(0, 1, 100):
    colors.append((255. / 255, 13. / 255, 87. / 255, l))
red_transparent_blue = LinearSegmentedColormap.from_list("red_transparent_blue", colors)


# Returns two data loaders (objects of the class: torch.utils.data.DataLoader) that are
# used to load the background and test datasets.
def prepare_dataloaders(bg_csv, test_csv, bg_batch_size=8, test_batch_size=1, num_workers=1):
    """
    Attributes:
        bg_csv (str): The path to the background CSV file.
        test_csv (str): The path to the test data CSV file.
        bg_batch_size (int): The batch size of the background data loader
        test_batch_size (int): The batch size of the test data loader
        num_workers (int): The number of sub-processes to use for dataloader
    """

    # bg_model = torch.jit.load(bg_csv)
    # bg_model.eval()

    training_data = CNN_Data(bg_csv)
    test_data = CNN_Data(test_csv)

    train_dataloader = DataLoader(training_data, batch_size=bg_batch_size)
    test_dataloader = DataLoader(test_data, batch_size=test_batch_size)

    return train_dataloader, test_dataloader

    pass


# Generates SHAP values for all pixels in the MRIs given by the test_loader
def create_SHAP_values(bg_loader, test_loader, mri_count, save_path):
    """
    Attributes:
        bg_loader (torch.utils.data.DataLoader): Dataloader instance for the background dataset.
        test_loader (torch.utils.data.DataLoader): Dataloader instance for the test dataset.
        mri_count (int): The total number of explanations to generate.
        save_path (str): The path to save the generated SHAP values (as .npy files).
    """
    # YOUR CODE HERE
    mri_count = max(mri_count, 1)
    images, _, _ = next(iter(bg_loader))
    print(images.shape)
    background = torch.unsqueeze(images, 1)
    print(background.shape)
    e = shap.DeepExplainer(model=model, data=background)

    test_gen = iter(test_loader)
    for i in range(min(len(test_loader), mri_count)):
        test_image, name, _ = next(test_gen)
        name = name[0].split("/")[-1]
        test_image = torch.unsqueeze(test_image, 0)
        shap_values = e.shap_values(test_image)
        shap_values = np.array(shap_values)
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        np.save(file=os.path.join(save_path + name), arr=shap_values)

    pass


# Aggregates SHAP values per brain region and returns a dictionary that maps
# each region to the average SHAP value of its pixels. 
def aggregate_SHAP_values_per_region(shap_values, seg_path, brain_regions=brain_regions):
    """
    Attributes:
        shap_values (ndarray): The shap values for an MRI (.npy).
        seg_path (str): The path to the segmented MRI (.nii).
        brain_regions (dict): The regions inside the segmented MRI image (see data_utl.py)
    """
    # YOUR CODE HERE
    contribution_map = {}
    # n=nib.load("../data/datasets/ADNI3/seg/ADNI_135_S_6510_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190823121302839_11_S863934_I1215774.nii")
    n = nib.load(seg_path)
    region_map = n.get_fdata()
    shap_values = shap_values[0][0]
    for i in range(len(shap_values)):
        for j in range(len(shap_values[0])):
            for k in range(len(shap_values[0][0])):
                val = shap_values[i][j][k]
                if val == 0:
                    continue
                else:
                    key = region_map[i][j][k]
                    origin = contribution_map.get(key, 0)
                    contribution_map[key] = origin + val
    ans = []

    index = 0
    for i in sorted(contribution_map.items(), key=lambda x: x[1], reverse=True):
        if index == 10:
            break
        if i[0] == 0:
            continue
        else:
            index += 1
            ans.append([i[0], brain_regions[i[0]], i[1]])
    return ans

    pass


# Returns a list containing the top-10 most contributing brain regions to each predicted class (AD/NotAD).
def output_top_5_lst(content, csv_file):
    """
    Attribute:
        csv_file (str): The path to a CSV file that contains the aggregated SHAP values per region.
    """
    # Region number	region	value
    df=pd.DataFrame(content,columns =['Region number', 'region', 'value'])
    df.to_csv(csv_file,index=False)
    # YOUR CODE HERE
    pass


# Plots SHAP values on a 2D slice of the 3D MRI.
def plot_shap_on_mri(subject_mri, shap_values, ad):
    """
    Attributes:
        subject_mri (str): The path to the MRI (.npy).
        shap_values (str): The path to the SHAP explanation that corresponds to the MRI (.npy).
    """
    # YOUR CODE HERE

    mri_np = np.load(subject_mri)
    mri_np = np.expand_dims(mri_np, -1)
    mri_np = np.expand_dims(mri_np, 0)

    shap_np = np.load(shap_values)
    print("shap" + str(shap_np.shape))
    print("mri" + str(mri_np.shape))

    shap_np = shap_np[ad, 0, 0]
    shap_np = np.expand_dims(shap_np, -1)
    shap_np = np.expand_dims(shap_np, 0)

    print(shap_np.shape)
    print(mri_np.shape)

    path = "../output"

    side = shap.image_plot(shap_values=np.rot90(shap_np[:, 91]), pixel_values=np.rot90(mri_np[:, 91]), show=False)
    plt.savefig(os.path.join(path, 'side' + str(ad) + '.png'))

    forward = shap.image_plot(shap_values=np.rot90(shap_np[:, :, 109]), pixel_values=np.rot90(mri_np[:, :, 109]),
                              show=False)
    plt.savefig(os.path.join(path, 'forward' + str(ad) + '.png'))

    upper = shap.image_plot(shap_values=shap_np[:, :, :, 91], pixel_values=mri_np[:, :, :, 91], show=False)
    plt.savefig(os.path.join(path, 'upper' + str(ad) + '.png'))
    pass


def task1(loaders, outputPath):
    correct = 0
    total = 0
    # since we're not training, we don't need to calculate the gradients for our outputs
    model.eval()
    wrong = 0
    right = 0
    for load in loaders:
        with torch.no_grad():
            for data in load:
                mri_file, path, label = data
                outputs = model(torch.unsqueeze(mri_file, 1))
                ans = 1
                if outputs[0][0] > outputs[0][1]:
                    ans = 0
                if label == ans:
                    right += 1
                else:
                    wrong += 1
    schema = ("Classified", "value")
    rows = [schema]
    rows.append(("Correct", right))
    rows.append(("Incorrect", wrong))
    f = open(outputPath, "w")
    f.writelines("Classified,value\n")
    f.writelines("Correct," + str(right) + "\n")
    f.writelines("Incorrect," + str(wrong) + "\n")
    f.close()


def task3(input_folder, output_folder):
    ad0_mri_name = os.path.join(input_folder,
                                "ADNI_135_S_6510_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190823121302839_11_S863934_I1215774.npy")
    ad1_mri_name = os.path.join(input_folder,
                                "ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.npy")
    ad0_shap_name = os.path.join(output_folder,
                                 "ADNI_135_S_6510_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190823121302839_11_S863934_I1215774.npy")
    ad1_shap_name = os.path.join(output_folder,
                                 "ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.npy")

    plot_shap_on_mri(subject_mri=ad0_mri_name, shap_values=ad0_shap_name, ad=0)
    plot_shap_on_mri(subject_mri=ad1_mri_name, shap_values=ad1_shap_name, ad=1)


def task4(nii_folder, shap_folder):
    shapv0 = np.load(os.path.join(shap_folder,
                                  "ADNI_135_S_6510_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190823121302839_11_S863934_I1215774.npy"))[0]
    nii0p = os.path.join(nii_folder,
                         "ADNI_135_S_6510_MR_Accelerated_Sag_IR-FSPGR___br_raw_20190823121302839_11_S863934_I1215774.nii")
    op0 = aggregate_SHAP_values_per_region(shapv0, nii0p)

    shapv1 = np.load(os.path.join(shap_folder,
                                  "ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.npy"))[1]
    nii1p = os.path.join(nii_folder,
                         "ADNI_022_S_6013_MR_Sagittal_3D_Accelerated_MPRAGE_br_raw_20190314145101831_129_S806245_I1142379.nii")
    op1 = aggregate_SHAP_values_per_region(shapv1, nii1p)

    path = "../output/task4"
    if not os.path.exists(path):
        os.mkdir(path)
    output_top_5_lst(op0, os.path.join(path, 'task-4-false.csv'))
    output_top_5_lst(op1, os.path.join(path, 'task-4-true.csv'))

    pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--task", type=int, help="[1/2/3/4]", default=4)
    parser.add_argument("-df", "--dataFolder", type=str, help="[path to the ADNI3 folder]",
                        default='../data/datasets/ADNI3/')
    parser.add_argument("-of", "--outputFolder", type=str,
                        help="[path to the output folder where we will store the final outputs]",
                        default='../output/')
    args = parser.parse_args()
    if not os.path.exists(args.outputFolder):
        os.mkdir(args.outputFolder)

    split_csv(csv_file=os.path.join(args.dataFolder, "ADNI3.csv"), output_folder=args.outputFolder)
    train_dataloader, test_dataloader = prepare_dataloaders(bg_csv=os.path.join(args.outputFolder, "bg_file.csv"),
                                                            test_csv=os.path.join(args.outputFolder, "test.csv"))
    model = model._CNN(20, 0.6)
    checkpoint = torch.load(os.path.join(args.dataFolder, 'cnn_best.pth'), map_location=torch.device('cpu'))
    model.load_state_dict(checkpoint["state_dict"])
    # TASK I: Load CNN model and instances (MRIs)
    #         Report how many of the 19 MRIs are classified correctly
    # YOUR CODE HERE
    if args.task == 1:
        task1([test_dataloader], os.path.join(args.outputFolder, "task-1.csv"))


    # TASK II: Probe the CNN model to generate predictions and compute the SHAP
    #          values for each MRI using the DeepExplainer or the GradientExplainer. 
    #          Save the generated SHAP values that correspond to instances with a
    #          correct prediction into output/SHAP/data/
    # YOUR CODE HERE
    elif args.task == 2:
        create_SHAP_values(train_dataloader, test_dataloader, 5,
                           os.path.join(args.outputFolder, "task2/"))
    # TASK III: Plot an explanation (pixel-based SHAP heatmaps) for a random MRI.
    #           Save heatmaps into output/SHAP/heatmaps/
    # YOUR CODE HERE 

    elif args.task == 3:
        task3(args.dataFolder, os.path.join(args.outputFolder, "task2/"))
    elif args.task == 4:
        task4(shap_folder=os.path.join(args.outputFolder, "task2/"), nii_folder=os.path.join(args.dataFolder, 'seg/'))

    # TASK IV: Map each SHAP value to its brain region and aggregate SHAP values per region.
    #          Report the top-10 most contributing regions per class (AD/NC) as top10_{class}.csv
    #          Save CSV files into output/top10/
    # YOUR CODE HERE 

    pass
