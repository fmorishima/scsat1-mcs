
import yaml
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from lxml import etree
from pathlib import Path
from yamcs.client import YamcsClient

EPS_GENERAL_HK_FILE_NO = 10
EPS_STARTUP_HK_FILE_NO = 7
COMMAND_INTERVAL_MS = 300
FILE_DOWNLOAD_INTERVAL_MS = 200
FILE_DOWNLOAD_MARGIN_MS = 10000
SYSHK_SKIP_NUM = 0

def LOG(message):
    now = datetime.now().strftime('%H:%M:%S.%f')[:-2]
    print(f'[{now}] {message}')


def format_time(timestamp):
    return time.strftime('%H:%M:%S', time.localtime(timestamp))


def load_yaml_config(filepath):
    with open(filepath, 'r') as f:
        return yaml.safe_load(f)

def handler(signum, frame):
    print("\n Interrupt signal has been received, termination...")
    sys.exit(0)

signal.signal(signal.SIGINT, handler)

def parse_xtce_command_definitions(xml_paths):
    # Extract all command definitions and their arguments from XML (XTCE format)
    command_definitions = {}
    for path in xml_paths:
        tree = etree.parse(str(path))
        root = tree.getroot()
        namespace = {'xtce': root.nsmap[None]}
        subsystem = root.attrib.get("name", "UNKNOWN")

        for cmd in root.xpath(".//xtce:MetaCommand", namespaces=namespace):
            cmd_name = cmd.attrib.get("name")
            fq_name = f"{subsystem}.{cmd_name}"
            params = {}
            for arg in cmd.xpath(".//xtce:Argument", namespaces=namespace):
                arg_name = arg.attrib["name"]
                param_type = arg.attrib.get("typeRef", "string")
                params[arg_name] = param_type
            command_definitions[fq_name] = params
    return command_definitions


def validate_commands(yaml_config, command_definitions):
    # Check if command names and parameters in the YAML match those in the XML definitions
    errors = []
    for cmd in yaml_config["commands"]:
        name = cmd["name"].replace("/", ".")
        params = cmd.get("params", {})

        if name not in command_definitions:
            errors.append(f"Undefined command: {name}")
            continue

        expected_params = command_definitions[name]
        unknown = set(params.keys()) - set(expected_params.keys())
        missing = set(expected_params.keys()) - set(params.keys())

        if unknown:
            errors.append(f"Invalid parameters in {name}: {unknown}")
        if missing:
            errors.append(f"Missing parameters in {name}: {missing}")
    return errors

def eps_set_time(processor):
    timestamp = time.time()
    name = "SCSAT1/EPS/REQ_SET_TIME"
    try:
        processor.issue_command(
                name,
                args={
                    "command_id": "1",
                    "timestamp": int(timestamp)
                }
                )
    except Exception as e:
        LOG(f"Error sending {name}: {e}")

    LOG(f"Issue: EPS set time request, time: {format_time(timestamp)}")
    time.sleep(COMMAND_INTERVAL_MS / 1000)

def request_eps_file_info(processor, file_no):
    name = "SCSAT1/EPS/REQ_FILE_INFO"
    try:
        processor.issue_command(
                name,
                args={
                    "command_id": "0",
                    "file_id": file_no
                }
                )
    except Exception as e:
        LOG(f"Error sending {name}: {e}")
    time.sleep(COMMAND_INTERVAL_MS / 1000)

def find_latest_one(archive, parameter, search_start_time):
    latest_time = None
    latest_id = None
    now = datetime.now(tz=timezone.utc)

    try:
        stream = archive.stream_parameter_values(parameter, start=search_start_time, stop=now)

        for pdata in stream:
            for data in pdata:
                value = data.eng_value
                gen_time = data.generation_time

                if latest_time is None or gen_time > latest_time:
                    latest_time = gen_time
                    latest_id = value

        if latest_id is None:
            LOG(f"ERROR: Could not find last ID, param: {parameter}")

    except Exception as e:
        LOG(f"ERROR: failed to find information: {e}")
        latest_id = None

    return latest_id

def eps_download_hk_history(processor, archive, file_no, start_entry_id):

    WAIT_REPLY_TIME_S = 1
    MAX_ENTRY_NUM = 880

    time_command_issued = datetime.now(tz=timezone.utc)
    LOG(f"Issue: File info request, file no : {file_no}")
    request_eps_file_info(processor, file_no)
    time.sleep(WAIT_REPLY_TIME_S)

    last_entry_id = find_latest_one(archive, "/SCSAT1/EPS/LAST_ENTRY_ID", time_command_issued)
    if last_entry_id is None:
        LOG("ERROR: Failed to find the latest entry ID")
        return None

    LOG(f"Found: Last entry ID, file no: {file_no}, ID: {last_entry_id}")
    if start_entry_id == 0: # skip download when first time
        LOG(f"Skip: file download request due to first time, start_entry_id: {start_entry_id}")
        return last_entry_id

    if last_entry_id > start_entry_id:
        name = "SCSAT1/EPS/REQ_FILE_DOWNLOAD"
        count = last_entry_id - start_entry_id
        if (count > MAX_ENTRY_NUM ):
            LOG(f"!Warning: requested count over MAX, count: {count}, MAX: {MAX_ENTRY_NUM}")
            LOG(f"!Warning: start entry id changed from: {start_entlry_id}, to: {last_entry_id - MAX_ENTRY_NUM}")
            start_entry_id = last_entry_id - MAX_ENTRY_NUM
            count = MAX_ENTRY_NUM

        LOG(f"Issue: File download request, file id: {file_no}, start: {start_entry_id}, end: {last_entry_id}, count: {count}")
        try:
            processor.issue_command(
                    name,
                    args={
                        "command_id": "1",
                        "status": "0",
                        "first_offset": "0",
                        "period_ms": FILE_DOWNLOAD_INTERVAL_MS,
                        "duration_s": "120",
                        "pkt_size": "244",
                        "file_id": file_no,
                        "start": start_entry_id,
                        "end": last_entry_id
                    }
                    )
        except Exception as e:
            LOG(f"Error sending {name}: {e}")
        time.sleep(FILE_DOWNLOAD_INTERVAL_MS * count / 1000 + FILE_DOWNLOAD_MARGIN_MS / 1000)
    else:
        LOG(f"Skip: No new entries to download for file no: {file_no}")

    return last_entry_id

def main_system_hk_history(processor, archive, start_seq_no):
    MAX_HISTORY_NUM = 1024 # two hours, it takes two min to download w/ 100ms
    start = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
    last_seq_no = find_latest_one(archive, '/SCSAT1/MAIN/SYSTEM_HK_SEQ_NO', start) - 1
    LOG(f"Found: Last sequence No., No: {last_seq_no}")

    if start_seq_no == 0: # skip download when first time
        LOG(f"Skip: System HK history request due to first time, start_seq_no: {start_seq_no}")
        return last_seq_no

    if last_seq_no is not None and last_seq_no > start_seq_no:
        name = "SCSAT1/MAIN/GET_SYSTEM_HK_HISTORY"
        count = min(last_seq_no - start_seq_no - 1, MAX_HISTORY_NUM)
        LOG(f"Issue: System HK history request, name: {name}, seq no: {last_seq_no}, count: {count}")
        try:
            processor.issue_command(
                    name,
                    args={
                        "command_id": "1",
                        "start_seq_no": start_seq_no,
                        "end_seq_no": last_seq_no,
                        "skip": SYSHK_SKIP_NUM,
                        "interval": FILE_DOWNLOAD_INTERVAL_MS
                    }
                    )
        except Exception as e:
            LOG(f"Error sending {name}: {e}")
        time.sleep(FILE_DOWNLOAD_INTERVAL_MS * count / 1000 + FILE_DOWNLOAD_MARGIN_MS / 1000)

    return last_seq_no


def send_commands(yaml_config, yamcs_url, instance):
    # Loop through commands and send them to Yamcs with specified delays
    client = YamcsClient(yamcs_url)
    processor = client.get_processor(instance, "realtime")
    archive = client.get_archive(instance)

    eps_general_hk_last = 0
    eps_startup_hk_last = 0
    main_system_hk_last = 0 # get it from yamcs db

    LOG(f"PARAMETERS:")
    LOG(f"- command interval: {COMMAND_INTERVAL_MS} msec")
    LOG(f"- file download interval: {FILE_DOWNLOAD_INTERVAL_MS} msec")
    LOG(f"- file download margin: {FILE_DOWNLOAD_MARGIN_MS} msec")
    LOG(f"- syshk download skip number: {SYSHK_SKIP_NUM}")

    loop_interval = yaml_config.get("loop_interval_s", 0)
    LOG(f"- Loop interval: {loop_interval} seconds")
    loop_count = 1

    while True:
        LOG(f"---------- Loop count: {loop_count} ----------")
        loop_start = time.perf_counter()

        # send commands defined in yaml
        for cmd in yaml_config["commands"]:
            name = "/SCSAT1/" + cmd["name"]
            params = cmd.get("params", {})
            wait = cmd.get("wait", 0) / 1000.0

            LOG(f"Issue: {name} {params}")
            try:
                processor.issue_command(
                        name,
                        args={k: str(v) for k, v in params.items()}
                        )
            except Exception as e:
                LOG(f"Error sending {name}: {e}")
            time.sleep(wait)

        # set EPS date
        eps_set_time(processor)

        # download eps file to get stored startup / general hk
        last_entry_id = eps_download_hk_history(processor, archive, EPS_GENERAL_HK_FILE_NO, eps_general_hk_last)
        if last_entry_id is not None:
            eps_general_hk_last = last_entry_id

        last_entry_id = eps_download_hk_history(processor, archive, EPS_STARTUP_HK_FILE_NO, eps_startup_hk_last)
        if last_entry_id is not None:
            eps_startup_hk_last = last_entry_id

        # download main system hk history
        last_seq_no = main_system_hk_history(processor, archive, main_system_hk_last)
        if last_seq_no is not None:
            main_system_hk_last = last_seq_no

        time_spend = time.perf_counter() - loop_start
        time_left = max(loop_interval - time_spend, 0)
        LOG(f"Spend time: {time_spend:.1f} sec in this loop.")
        LOG(f"Waiting: {time_left:.1f} sec before next round.\n")
        time.sleep(time_left)
        loop_count += 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Yamcs Command Sender with YAML and XTCE validation")
    parser.add_argument("--config", default="commands.yaml", help="Path to YAML config file")
    parser.add_argument("--yamcs_url", default="http://localhost:8090", help="Yamcs server URL")
    parser.add_argument("--xml_dir", default=".", help="Directory where scsat1_*.xml files are located")
    parser.add_argument("--instance", default="scsat1", help="Yamcs instance name")
    args = parser.parse_args()

    config = load_yaml_config(args.config)
    xml_paths = list(Path(args.xml_dir).glob("scsat1_*.xml"))
    cmd_defs = parse_xtce_command_definitions(xml_paths)
    validation_errors = validate_commands(config, cmd_defs)

    if validation_errors:
        LOG("Validation errors:")
        for e in validation_errors:
            LOG("  " + e)
    else:
        LOG("---------- Start operation testing ----------")
        send_commands(config, args.yamcs_url, args.instance)
