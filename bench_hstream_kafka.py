#!/usr/bin/env python3

from pathlib import Path
import argparse
import glob
import os
import subprocess
import tempfile

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT", os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
)
SCENARIO_DIR = os.path.join(PROJECT_ROOT, "hstream-kafka", "scenarios")
RESULT_DIR = os.path.join(PROJECT_ROOT, "results")

loginfo = lambda s: print(f"\033[96m{s}\033[0m")


def get_scenarios():
    return [x.name for x in os.scandir(SCENARIO_DIR) if x.is_dir()]


def get_driver_tmpls(scenario):
    scenario_dir = os.path.join(SCENARIO_DIR, scenario)
    driver_tmpl_dir = os.path.join(scenario_dir, "driver-tmpls")
    return [
        driver_tmpl
        for driver_tmpl in os.scandir(driver_tmpl_dir)
        if driver_tmpl.is_file()
    ]


def get_workloads(scenario):
    scenario_dir = os.path.join(SCENARIO_DIR, scenario)
    workloads_dir = os.path.join(scenario_dir, "workloads")
    return [x for x in os.scandir(workloads_dir) if x.is_file()]


def get_results(result_dir):
    return [
        path
        for path in glob.glob(
            os.path.join(result_dir, "*.json"), recursive=False
        )
    ]


def setup_drivers(driver_tmpls, scenario, gen_dir, overrides):
    driver_dir = os.path.join(gen_dir, scenario, "drivers")
    Path(driver_dir).mkdir(parents=True, exist_ok=True)

    def gen_driver():
        for driver_tmpl in driver_tmpls:
            with open(driver_tmpl.path, "r") as fr:
                tmpl_contents = fr.read()
                for k, v in overrides.items():
                    tmpl_contents = tmpl_contents.replace(k, v)
                driver_filepath = os.path.join(driver_dir, driver_tmpl.name)
                with open(driver_filepath, "w") as fw:
                    fw.write(tmpl_contents)
                    yield driver_filepath

    return list(gen_driver())


def main():
    all_scenarios = get_scenarios()
    parser = argparse.ArgumentParser(description="Bench hstream kafka.")
    parser.add_argument(
        "--kafka",
        required=True,
        help="kafka bootstrap.servers, e.g. localhost:9092,localhost:9093",
    )
    parser.add_argument(
        "--hstream",
        required=True,
        help="hstream bootstrap.servers, e.g. localhost:9092,localhost:9093",
    )
    parser.add_argument(
        "-s",
        "--scenario",
        default=None,
        choices=all_scenarios,
        nargs="*",
        help="Bench scenarios, default: all",
    )
    parser.add_argument(
        "-d",
        "--driver",
        default=None,
        nargs="*",
        help=("Drivers under the scenario, default: all"),
    )
    parser.add_argument(
        "-w",
        "--workload",
        default=None,
        nargs="*",
        help=("Workloads under the scenario, default: all"),
    )
    parser.add_argument(
        "-o",
        "--result-dir",
        default="./results",
        type=Path,
        help="Where to put the benchmark results, default: ./results",
    )
    args = vars(parser.parse_args())
    kafka_driver_overrides = {
        "__NAME__": "kafka",
        "__BOOTSTRAP_SERVERS__": args["kafka"],
    }
    hstream_driver_overrides = {
        "__NAME__": "hstream-kafka",
        "__BOOTSTRAP_SERVERS__": args["hstream"],
    }
    result_dir = args["result_dir"]
    scenarios = args["scenario"] if args["scenario"] else all_scenarios
    for s in scenarios:
        with tempfile.TemporaryDirectory() as d:
            kafka_dir = os.path.join(d, "kafka")
            Path(kafka_dir).mkdir(parents=True, exist_ok=True)
            hstream_dir = os.path.join(d, "hstream")
            Path(hstream_dir).mkdir(parents=True, exist_ok=True)
            out_dir = os.path.join(result_dir, s)
            Path(out_dir).mkdir(parents=True, exist_ok=True)

            driver_tmpls = get_driver_tmpls(s)
            if args["driver"]:
                driver_tmpls = [
                    d for d in driver_tmpls if d.name in args["driver"]
                ]
            kafka_drivers = setup_drivers(
                driver_tmpls, s, kafka_dir, kafka_driver_overrides
            )
            hstream_drivers = setup_drivers(
                driver_tmpls, s, hstream_dir, hstream_driver_overrides
            )
            workloads = get_workloads(s)
            if args["workload"]:
                workloads = [w for w in workloads if w.name in args["workload"]]

            cmd_drivers = ",".join(kafka_drivers + hstream_drivers)
            cmd_workloads = " ".join(workloads)
            bench_cmd = (
                f"bin/benchmark -d {cmd_drivers} -od {out_dir} {cmd_workloads}"
            )
            loginfo(f"Run command: {bench_cmd}")
            subprocess.run(bench_cmd, shell=True, check=True)
            results = get_results(out_dir)
            if results:
                draw_cmd = (
                    f"bin/create_charts.py -o {out_dir} -r {' '.join(results)}"
                )
                loginfo(f"Run command: {draw_cmd}")


if __name__ == "__main__":
    main()
