import argparse
import json
import os

from multiprocessing import Pool

import cv2

from tqdm import tqdm


mead_data_root = "/D_data/Front/data/MEAD_frames"
mead_data_out_root = "/D_data/Front/data/MEAD_frames_names"
os.makedirs(mead_data_out_root, exist_ok=True)

id_names = [
    "M003_frames",
    "M005_frames",
    "M007_frames",
    "M009_frames",
    "M011_frames",
    "M012_frames",
    "M013_frames",
    "M019_frames",
    "M022_frames",
    "M023_frames",
    "M024_frames",
    "M025_frames",
    "M026_frames",
    "M027_frames",
    "M028_frames",
    "M029_frames",
    "M030_frames",
    "M031_frames",
    "M032_frames",
    "M033_frames",
    "M034_frames",
    "M035_frames",
    "M037_frames",
    "M039_frames",
    "M040_frames",
    "M041_frames",
    "M042_frames",
    "W009_frames",
    "W011_frames",
    "W014_frames",
    "W015_frames",
    "W016_frames",
    "W017_frames",
    "W018_frames",
    "W019_frames",
    "W021_frames",
    "W023_frames",
    "W024_frames",
    "W025_frames",
    "W026_frames",
    "W028_frames",
    "W029_frames",
    "W033_frames",
    "W035_frames",
    "W036_frames",
    "W037_frames",
    "W038_frames",
    "W040_frames",
]


def main(args):
    cur_job_id_names = id_names[args.job_idx :: args.num_jobs]
    for id_name in tqdm(cur_job_id_names, desc="cur_job_id_names", total=len(cur_job_id_names), leave=False):
        cmd = "export AZCOPY_CRED_TYPE=Anonymous;"
        cmd += "export AZCOPY_CONCURRENCY_VALUE=AUTO;"
        cmd += f"/D_data/AML/azcopy_linux_amd64_10.17.0/azcopy copy 'https://msramcg.blob.core.windows.net/yuegao/Front/data/MEAD_frames/{id_name}/?sv=2021-10-04&st=2023-02-10T02%3A11%3A22Z&se=2023-02-17T02%3A11%3A22Z&sr=c&sp=rl&sig=3yPY3%2Fr2de6AWzmr07PxUUWx5ZEdNkG5TDl5RkR1PNI%3D' '/D_data/Front/data/MEAD_frames' --overwrite=true --check-md5 FailIfDifferent --from-to=BlobLocal --recursive --log-level=NONE;"
        cmd += "unset AZCOPY_CRED_TYPE;"
        cmd += "unset AZCOPY_CONCURRENCY_VALUE;"

        os.system(cmd)

        cur_id_all_frames = {}
        cur_id_all_frames["frames"] = []

        angle_names = os.listdir(os.path.join(mead_data_root, id_name))
        for angle_name in tqdm(angle_names, desc=id_name, leave=False):
            expression_names = os.listdir(os.path.join(mead_data_root, id_name, angle_name))
            for expression_name in tqdm(expression_names, desc=angle_name, leave=False):
                degree_names = os.listdir(os.path.join(mead_data_root, id_name, angle_name, expression_name))
                for degree_name in tqdm(degree_names, desc=expression_name, leave=False):
                    clips = os.listdir(os.path.join(mead_data_root, id_name, angle_name, expression_name, degree_name))
                    for clip in tqdm(clips, desc=degree_name, leave=False):
                        frames = os.listdir(
                            os.path.join(mead_data_root, id_name, angle_name, expression_name, degree_name, clip)
                        )
                        for frame in tqdm(frames, desc=clip, leave=False):
                            frame_path = os.path.join(
                                mead_data_root, id_name, angle_name, expression_name, degree_name, clip, frame
                            )
                            try:
                                img = cv2.imread(frame_path)
                                if img is not None or img.size != 0:
                                    cur_id_all_frames["frames"].append(frame_path)
                            except:
                                continue

        cur_id_all_frame_path = os.path.join(mead_data_out_root, id_name, "all_frames.json")
        with open(cur_id_all_frame_path, "w") as f:
            json.dump(cur_id_all_frames, f)

        os.system(f"rm -rf /D_data/Front/data/MEAD_frames/{id_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Process the downloaded MEAD tar files""")
    parser.add_argument("--job_idx", type=int, default=0)
    parser.add_argument("--job_nums", type=int, default=2)

    args = parser.parse_args()
    main(args)
