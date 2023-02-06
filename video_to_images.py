import argparse
import json
import multiprocessing as mp
import os

from multiprocessing import Pool

from tqdm import tqdm


def cmd_wrapper(cmd):
    os.system(cmd)


videos_folder = "../MEAD_extracted"
frames_folder = "../MEAD_frames"
processed_folder = "../MEAD_processed"
video_files = os.path.join(processed_folder, "video_files.json")


with open(video_files, "r") as f:
    video_files_dict = json.load(f)

videos = video_files_dict["videos"]
total_videos = len(videos)
print("total videos:", total_videos)


def main(job_idx, num_jobs, threads):

    cur_job_videos = videos[job_idx::num_jobs]
    one_process_jobs = len(cur_job_videos) // threads + 1

    def one_process(process_id):
        for n in tqdm(
            range(process_id * one_process_jobs, (process_id + 1) * one_process_jobs), desc=f"process {process_id}"
        ):
            if n >= len(cur_job_videos):
                break
            video = cur_job_videos[n]
            video_path = os.path.join(videos_folder, video)
            out_dir = os.path.join(frames_folder, video.split(".")[0])
            out_dir = out_dir.replace("video", "frames")
            os.makedirs(out_dir, exist_ok=True)
            cmd = f"ffmpeg -y -i {video_path} -hide_banner -loglevel error -qscale:v 1 -qmin 1 -qmax 1 -vsync 0 {out_dir}/%06d.png"
            os.system(cmd)

    processes = [mp.Process(target=one_process, args=(process_id,)) for process_id in range(threads)]
    # Run processes
    for p in processes:
        p.start()

    # Exit the completed processes
    for p in processes:
        p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("""Process the downloaded MEAD tar files""")
    parser.add_argument("--job_idx", type=int, default=0)
    parser.add_argument("--num_jobs", type=int, default=1)
    parser.add_argument("--threads", type=int, default=48)

    args = parser.parse_args()
    main(args.job_idx, args.num_jobs, args.threads)
