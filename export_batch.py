import os
import re
import math
import time
import shutil
import logging
import subprocess
from tempfile import mkdtemp

from functional import seq

LAST_EXPORTED_PATH = '/home/ubuntu/last_exported_ethereum_block'
GETH_LOG_PATH = '/home/ubuntu/geth.log'
LOGGER = logging.getLogger("endor")
formatter = logging.Formatter('[%(levelname)s][%(asctime)s][%(funcName)s]: %(message)s')
handlers = [logging.FileHandler("export.log"), logging.StreamHandler()]
for handler in handlers:
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)


def build_export_batches(start_block, end_block, blocks_per_batch):
    return seq(chunks(range(start_block, end_block + 1), blocks_per_batch)).map(lambda x: (min(x), max(x))).to_list()


def get_last_fetched_block():
    number_regex = re.compile("(?<=number=)(\d+)")
    results = []
    p = subprocess.Popen('journalctl -u geth --no-pager -n 20', shell=True, stdout=subprocess.PIPE)
    p.wait()
    lines = p.stdout.readlines()
    for line in lines:
        results.extend(number_regex.findall(line))
    return max(int(i) for i in results)


def get_last_exported_block():
    with open(LAST_EXPORTED_PATH) as f:
        return int(f.read().strip())


def start_logs_fetcher(batch_start, batch_end, logs_directory):
    LOGGER.info("Exporting logs {} - {}".format(batch_start, batch_end))
    return subprocess.Popen([
        "java", "-cp", "/home/ubuntu/fetcher.jar", "com.endor.spark.blockchain.ethereum.token.logsfetcher.LogsFetcher",
        "fetch", "--communicationMode", "http 127.0.0.1:8545",
        "--fromBlock", str(batch_start), "--toBlock", str(batch_end),
        "--topics", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "--output", os.path.join(logs_directory, "{}-{}.json".format(batch_start, batch_end))])


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def export_blocks(first_export, last_export):
    LOGGER.debug("Killing geth...")
    subprocess.call("sudo systemctl stop geth", shell=True)
    LOGGER.debug("Done")
    time.sleep(5)
    export_batches = build_export_batches(first_export, last_export, 50)
    export_directory = mkdtemp()
    LOGGER.debug("blocks directory {}".format(export_directory))
    for batch_start, batch_end in export_batches:
        LOGGER.info("Exporting {} - {}".format(batch_start, batch_end))
        subprocess.Popen(["geth", "export", os.path.join(export_directory, "{}-{}.bin".format(batch_start, batch_end)),
                          str(batch_start), str(batch_end)]).wait()
        LOGGER.info("Done")
    subprocess.call("sudo systemctl start geth", shell=True)
    return export_directory


def export_logs(first_export, last_export):
    logs_directory = mkdtemp()
    LOGGER.debug("logs directory {}".format(logs_directory))
    logs_export_batches = build_export_batches(first_export, last_export, 1000)
    for chunk in chunks(logs_export_batches, 5):
        processes = [start_logs_fetcher(batch[0], batch[1], logs_directory) for batch in chunk]
        for process in processes:
            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, "")
    return logs_directory


def export():
    try:
        first_export = get_last_exported_block() + 1
        last_export = get_last_fetched_block() - 10
        LOGGER.info("Exporting between {} - {}".format(first_export, last_export))
        logs_directory = export_logs(first_export, last_export)
        blocks_export_directory = export_blocks(first_export, last_export)
        LOGGER.info("Uploading blocks")
        subprocess.call('aws s3 sync {} s3://endor-blockchains/ethereum/blocks/Inbox'.format(blocks_export_directory),
                        shell=True)
        LOGGER.info("Uploading logs")
        subprocess.call('aws s3 sync {} s3://endor-blockchains/ethereum/logs/Inbox'.format(logs_directory), shell=True)
        with open(LAST_EXPORTED_PATH, 'w') as f:
            f.write(str(last_export))
        LOGGER.info("Removing temp dirs")
        shutil.rmtree(blocks_export_directory)
        shutil.rmtree(logs_directory)
    except:
        LOGGER.error("Exception in main", exc_info=True)


if __name__ == "__main__":
    export()
