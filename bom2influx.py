import json
import urllib3
import argparse
from influxdb import InfluxDBClient
import logging
from time import sleep
from datetime import datetime, timedelta  

# Setup the logging
logging.basicConfig(format="%(asctime)s: %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_log = logging.getLogger()
_log.setLevel("INFO")

headers = {'user-agent':'Mozilla/5.0 (Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0'}

field_keys = ['apparent_t', 'cloud_base_m', 'cloud_oktas', 'cloud_type_id', 'delta_t', 'gust_kmh', 'air_temp', 'dewpt', 'press', 'press_qnh', 'press_msl', 'rel_hum', 'swell_height', 'swell_period',  'wind_spd_kmh']
tag_keys = ['wmo','name','history_product','lat','lon','cloud','cloud_type','press_tend','rain_trace','sea_state','swell_dir_worded','vis_km','weather','wind_dir']
time_key = 'aifstime_utc'
sleep_time = 30*60
def main():
    parser = argparse.ArgumentParser(
        description="Bureau of Meterology 2 InfluxDB",
        prog="bom2influx",
        usage="%(prog)s [options]")

    parser.add_argument("--url", help="URL to BOM json files", nargs=1, default=[""],action='append')
    parser.add_argument("--host", help="URL influxdb", nargs=1, default=["localhost"])
    parser.add_argument("--port", help="influx port", nargs=1, default=["8086"])
    parser.add_argument("--db", help="influx database", nargs=1, default=["BOM"])
    parser.add_argument("--loop", help="influx database", nargs=1, default=[False])
    args, unknown = parser.parse_known_args()

    if unknown:
        _log.error(f'unknown arguments passed; {unknown}')

    urls = [vars(args)['url'][i][0] for i in range(1,len(vars(args)['url']))]
    host = vars(args)['host'][0]
    port = vars(args)['port'][0]
    db = vars(args)['db'][0]
    loop = vars(args)['loop'][0]

    http = urllib3.PoolManager(headers=headers)
    client = InfluxDBClient(host=host, port=port)
    
    client.create_database(db)
    client.switch_database(db)
    while True:
        json_body = []
        for url in urls:
            request = http.request('GET', url)
            result = json.loads(request.data.decode('utf8'))
            latest = result["observations"]["data"][0]
            tags = {tk:latest[tk] for tk in tag_keys}
            fields = {fk:latest[fk] for fk in field_keys}
            time = latest[time_key]
            _log.info(f'{latest["name"]} @ {url} data for {time}')
            json_body.append({"measurement": "observations",
            "tags":tags,
            "fields":fields,
            "time":time})
        client.write_points(json_body)
        next_reading = datetime.now() + timedelta(seconds = sleep_time)
        _log.info(f'Points written, next reading at {next_reading}')
        if not(loop):
            break
        sleep(sleep_time)

if __name__ == "__main__":
    main()


