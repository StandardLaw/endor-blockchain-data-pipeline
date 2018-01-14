import re
import os
import math
import shutil
import subprocess
from tempfile import mkdtemp

import sh
import time
from functional import seq

LAST_EXPORTED_PATH = '/home/ubuntu/last_exported_ethereum_block'
GETH_LOG_PATH = '/home/ubuntu/geth.log'

def build_export_batches(start_block, end_block, blocks_per_batch):
    return seq(range(int(math.ceil((end_block - start_block) / float(blocks_per_batch))))).map(lambda x: (x * blocks_per_batch + start_block, min([(x + 1) * blocks_per_batch + start_block - 1, end_block])))


def get_last_fetched_block():
    number_regex = re.compile("(?<=number=)(\d+)")
    results = []
    for line in sh.tail("-10", GETH_LOG_PATH):
        results.extend(number_regex.findall(line))
    return max(int(i) for i in results)

def get_last_exported_block():
    with open(LAST_EXPORTED_PATH) as f:
        return int(f.read().strip())

def start_logs_fetcher(batch_start, batch_end, logs_directory):
    print "Exporting logs {} - {}".format(batch_start, batch_end)
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

def export():
    first_export = get_last_exported_block() + 1
    last_export = get_last_fetched_block() - 10
    try:
        sh.pkill("geth").wait()
    except sh.ErrorReturnCode_1:
        raise
    time.sleep(5)
    export_batches = build_export_batches(first_export, last_export, 10000)
    export_directory = mkdtemp()
    logs_directory = mkdtemp()
    for batch_start, batch_end in export_batches:
        print "Exporting {} - {}".format(batch_start, batch_end)
        subprocess.Popen(["geth", "export", os.path.join(export_directory, "{}-{}.bin".format(batch_start, batch_end)), str(batch_start), str(batch_end)]).wait()
        print "Done"
    subprocess.Popen('nohup geth --rpc --rpcaddr 0.0.0.0 --cache 2048 --syncmode "full" > geth.log &', shell=True)
    time.sleep(10)
    logs_export_batches = build_export_batches(first_export, last_export, 1000)
    for chunk in chunks(logs_export_batches.to_list(), 5):
        processes = [start_logs_fetcher(batch[0], batch[1], logs_directory) for batch in chunk]
        for process in processes:
            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, "")
    subprocess.call('aws s3 sync {} s3://endor-blockchains/ethereum/blocks/Inbox'.format(export_directory), shell=True)
    subprocess.call('aws s3 sync {} s3://endor-blockchains/ethereum/logs/Inbox'.format(logs_directory), shell=True)
    with open(LAST_EXPORTED_PATH, 'w') as f:
        f.write(str(last_export))
    shutil.rmtree(export_directory)
    shutil.rmtree(logs_directory)

if __name__ == "__main__":
    export()
