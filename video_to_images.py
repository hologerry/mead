import argparse
import json
import os

from multiprocessing import Pool

from tqdm import tqdm


def cmd_wrapper(cmd):
    os.system(cmd)


videos_folder = "../MEAD_extracted"
frames_folder = "../MEAD_extracted_frames"
processed_folder = "../MEAD_processed"
video_files = os.path.join(processed_folder, "video_files.json")


with open(video_files, "r") as f:
    video_files_dict = json.load(f)

videos = video_files_dict["videos"]
total_videos = len(videos)
print("total videos:", total_videos)


def main(job_idx, num_jobs, threads):
    cur_job_videos = videos[job_idx::num_jobs]
    print(f"job [{job_idx}/{num_jobs}] started, processing {len(cur_job_videos)} videos")
    programs = []
    for video in tqdm(cur_job_videos):
        video_path = os.path.join(videos_folder, video)
        out_dir = os.path.join(frames_folder, video.split(".")[0])
        out_dir = out_dir.replace("video", "frames")
        os.makedirs(out_dir, exist_ok=True)
        cmd = f"ffmpeg -i {video_path} -hide_banner -loglevel error -qscale:v 1 -qmin 1 -qmax 1 -vsync 0 {out_dir}/%06d.png"
        programs.append(cmd)

    pool = Pool(threads)
    pool.map(cmd_wrapper, programs)
    pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Process the downloaded MEAD tar files""")
    parser.add_argument("--job_idx", type=int, default=0)
    parser.add_argument("--num_jobs", type=int, default=1)
    parser.add_argument("--threads", type=int, default=48)

    args = parser.parse_args()
    main(args.job_idx, args.num_jobs, args.threads)
