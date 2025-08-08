import ee
import geemap
import utils
import threading
import time
from typing import Optional
import os
import inspect
import datetime
from tqdm import tqdm

ee.Authenticate()
ee.Initialize(project='project-id')

EE_TASK_MONITORING_QUEUE: dict[dict: dict] = {}
EE_TASK_MONITORING_QUEUE_LOCK = threading.Lock()
EE_TASK_QUEUE: list[dict] = []
EE_TASK_QUEUE_LOCK = threading.Lock()

BAND_LIST = ee.List([
    ee.Dictionary({'tBreak': [
        'tBreak_0', 'tBreak_1', 'tBreak_2', 'tBreak_3', 'tBreak_4',
        'tBreak_5', 'tBreak_6', 'tBreak_7', 'tBreak_8', 'tBreak_9'
    ]}),
    ee.Dictionary({'changeProb': [
        'changeProb_0', 'changeProb_1', 'changeProb_2', 'changeProb_3', 'changeProb_4',
        'changeProb_5', 'changeProb_6', 'changeProb_7', 'changeProb_8', 'changeProb_9'
    ]}),
    ee.Dictionary({'Blue_magnitude': [
        'Blue_magnitude_0', 'Blue_magnitude_1', 'Blue_magnitude_2', 'Blue_magnitude_3', 'Blue_magnitude_4',
        'Blue_magnitude_5', 'Blue_magnitude_6', 'Blue_magnitude_7', 'Blue_magnitude_8', 'Blue_magnitude_9'
    ]}),
    ee.Dictionary({'Green_magnitude': [
        'Green_magnitude_0', 'Green_magnitude_1', 'Green_magnitude_2', 'Green_magnitude_3', 'Green_magnitude_4',
        'Green_magnitude_5', 'Green_magnitude_6', 'Green_magnitude_7', 'Green_magnitude_8', 'Green_magnitude_9'
    ]}),
    ee.Dictionary({'Red_magnitude': [
        'Red_magnitude_0', 'Red_magnitude_1', 'Red_magnitude_2', 'Red_magnitude_3', 'Red_magnitude_4',
        'Red_magnitude_5', 'Red_magnitude_6', 'Red_magnitude_7', 'Red_magnitude_8', 'Red_magnitude_9'
    ]}),
    ee.Dictionary({'NIR_magnitude': [
        'NIR_magnitude_0', 'NIR_magnitude_1', 'NIR_magnitude_2', 'NIR_magnitude_3', 'NIR_magnitude_4',
        'NIR_magnitude_5', 'NIR_magnitude_6', 'NIR_magnitude_7', 'NIR_magnitude_8', 'NIR_magnitude_9'
    ]}),
    ee.Dictionary({'SWIR1_magnitude': [
        'SWIR1_magnitude_0', 'SWIR1_magnitude_1', 'SWIR1_magnitude_2', 'SWIR1_magnitude_3', 'SWIR1_magnitude_4',
        'SWIR1_magnitude_5', 'SWIR1_magnitude_6', 'SWIR1_magnitude_7', 'SWIR1_magnitude_8', 'SWIR1_magnitude_9'
    ]}),
    ee.Dictionary({'SWIR2_magnitude': [
        'SWIR2_magnitude_0', 'SWIR2_magnitude_1', 'SWIR2_magnitude_2', 'SWIR2_magnitude_3', 'SWIR2_magnitude_4',
        'SWIR2_magnitude_5', 'SWIR2_magnitude_6', 'SWIR2_magnitude_7', 'SWIR2_magnitude_8', 'SWIR2_magnitude_9'
    ]}),
    ee.Dictionary({'NDVI_magnitude': [
        'NDVI_magnitude_0', 'NDVI_magnitude_1', 'NDVI_magnitude_2', 'NDVI_magnitude_3', 'NDVI_magnitude_4',
        'NDVI_magnitude_5', 'NDVI_magnitude_6', 'NDVI_magnitude_7', 'NDVI_magnitude_8', 'NDVI_magnitude_9'
    ]}),
    ee.Dictionary({'EVI_magnitude': [
        'EVI_magnitude_0', 'EVI_magnitude_1', 'EVI_magnitude_2', 'EVI_magnitude_3', 'EVI_magnitude_4',
        'EVI_magnitude_5', 'EVI_magnitude_6', 'EVI_magnitude_7', 'EVI_magnitude_8', 'EVI_magnitude_9'
    ]}),
    ee.Dictionary({'TCB_magnitude': [
        'TCB_magnitude_0', 'TCB_magnitude_1', 'TCB_magnitude_2', 'TCB_magnitude_3', 'TCB_magnitude_4',
        'TCB_magnitude_5', 'TCB_magnitude_6', 'TCB_magnitude_7', 'TCB_magnitude_8', 'TCB_magnitude_9'
    ]}),
    ee.Dictionary({'TCG_magnitude': [
        'TCG_magnitude_0', 'TCG_magnitude_1', 'TCG_magnitude_2', 'TCG_magnitude_3', 'TCG_magnitude_4',
        'TCG_magnitude_5', 'TCG_magnitude_6', 'TCG_magnitude_7', 'TCG_magnitude_8', 'TCG_magnitude_9'
    ]}),
    ee.Dictionary({'TCW_magnitude': [
        'TCW_magnitude_0', 'TCW_magnitude_1', 'TCW_magnitude_2', 'TCW_magnitude_3', 'TCW_magnitude_4',
        'TCW_magnitude_5', 'TCW_magnitude_6', 'TCW_magnitude_7', 'TCW_magnitude_8', 'TCW_magnitude_9'
    ]}),
])

START_DATE = ee.Date('2015-06-27')
END_DATE = ee.Date('2025-01-01')
AOI_GRID = ee.FeatureCollection('')
TP_FOREST_MASK: ee.Image = ee.Image('').select(['b1']).neq(0)
IMAGE_COLLECTION = sentinel_2_l2a = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
MAX_PARALLEL_TASKS = 4
CANCLE_TASK_TO_SPLIT = True
OUTPUT_COLLECTION = 'CCDC/ccdc_res'
SPLIT_BY = 2


def _log(self, msg):
    if not os.path.exists('./log'):
        os.makedirs('./log')
    with open('./log/log.log', 'a') as f:
        function_name = inspect.currentframe().f_back.f_code.co_name
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'[{timestamp}] [{function_name}]: {msg}\n')


def _log_err(self, msg):
    if not os.path.exists('./log'):
        os.makedirs('./log')
    with open('./log/err.log', 'a') as f:
        function_name = inspect.currentframe().f_back.f_code.co_name
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f'[{timestamp}] [{function_name}]: {msg}\n')


def log(msg):
    thread = threading.Thread(target=_log, args=(msg,)).start()


def log_err(msg):
    thread = threading.Thread(target=_log_err, args=(msg,)).start()


def append_ee_task_queue(task: ee.batch.Task, aoi: ee.Geometry, file_name: str, attempt: int):
    aoi_coords = aoi.coordinates()
    coords = ee.List(aoi_coords.get(0))
    xmin = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.min())
    ymin = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.min())
    xmax = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.max())
    ymax = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.max())

    xmin = ee.Number(xmin).getInfo()
    ymin = ee.Number(ymin).getInfo()
    xmax = ee.Number(xmax).getInfo()
    ymax = ee.Number(ymax).getInfo()

    while True:
        with EE_TASK_QUEUE_LOCK:
            if len(EE_TASK_QUEUE) < 250:
                break
        time.sleep(30)

    with EE_TASK_QUEUE_LOCK:
        EE_TASK_QUEUE.append({
            'task': task,
            'aoi_coords': {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax},
            'file_name': file_name,
            'attempt': attempt,
        })


def get_ee_task_queue() -> Optional[dict]:
    if len(EE_TASK_QUEUE) == 0:
        return None
    with EE_TASK_QUEUE_LOCK:
        return EE_TASK_QUEUE.pop(0)


def append_ee_task_monitoring_queue(task: ee.batch.Task, aoi_coords: ee.List, file_name: str, attempt: int):
    with EE_TASK_MONITORING_QUEUE_LOCK:
        EE_TASK_MONITORING_QUEUE[task.id] = {
            # To cut current aoi into smaller pieces
            'aoi_coords': aoi_coords,

            # To get the task info, the three fields are necessary
            'id': task.id,
            'state': ee.batch.Task.State(task.status()['state']),
            'type': ee.batch.Task.Type(task.status()['task_type']),

            'file_name': file_name,
            'attempt': attempt,
        }


def ccdc_image_collection_preprocess(aoi: ee.Geometry) -> ee.ImageCollection:
    img_col = IMAGE_COLLECTION.filterBounds(aoi) \
        .filterDate(START_DATE, END_DATE)
    img_col = img_col.map(lambda img: img.clip(aoi))
    img_col = img_col.remove_clouds()
    img_col = img_col.band_rename()
    img_col = img_col.map(lambda img: img.ndvi())
    img_col = img_col.map(lambda img: img.evi())
    img_col = img_col.map(lambda img: img.kt_transform())
    ret = img_col.select(
        ['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2', 'NDVI', 'EVI', 'TCB', 'TCG', 'TCW']
    )
    return ret


def ccdc(ccdc_input: ee.ImageCollection, aoi: ee.Geometry) -> ee.Image:
    ccdc_result: ee.Image = ee.Algorithms.TemporalSegmentation.Ccdc(
        ccdc_input,
        minObservations=12,
        dateFormat=1,
        chiSquareProbability=0.99,
        maxIterations=25000,
    )
    return ccdc_result


def ccdc_result_flaten(ccdc_result: ee.Image) -> ee.Image:
    ccdc_result_flat_list = BAND_LIST.map(
        lambda band: ccdc_result.select([ee.Dictionary(band).keys().get(0)]).arrayPad([10], 0) \
            .arrayFlatten([ee.Dictionary(band).values().get(0)])
    )
    ccdc_result_flat = ee.Image(ccdc_result_flat_list.get(0)).addBands(ee.Image(ccdc_result_flat_list.get(1))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(2))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(3))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(4))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(5))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(6))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(7))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(8))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(9))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(10))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(11))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(12)))
    return ccdc_result_flat


def ccdc_result_export(ccdc_result_flat: ee.Image, aoi: ee.Geometry, file_name: str, attempt: int):
    task = ee.batch.Export.image.toAsset(
        image=ccdc_result_flat,
        description='export_' + file_name,
        assetId=f'',
        scale=10,
        region=aoi.bounds(),
        maxPixels=1e13,
        crs='EPSG:4326',
    )
    append_ee_task_queue(task, aoi, file_name, attempt)


def start_one_task():
    task_dict = get_ee_task_queue()
    if task_dict is not None:
        task_dict['task'].start()
        append_ee_task_monitoring_queue(
            task_dict['task'], task_dict['aoi_coords'], task_dict['file_name'], task_dict['attempt']
        )
        print('Task', task_dict['task'].id, 'started')


def ccdc_main():
    index = 0
    for aoi_grid_feature in AOI_GRID.getInfo()['features']:
        aoi = ee.Feature(aoi_grid_feature['geometry']).geometry()
        ccdc_input = ccdc_image_collection_preprocess(aoi)
        ccdc_result = ccdc(ccdc_input, aoi)
        ccdc_result_flat = ccdc_result_flaten(ccdc_result)
        file_name = f'ccdc_result_{index}'
        attempt = 1
        ccdc_result_export(ccdc_result_flat, aoi, file_name, attempt)
        index += 1


def ee_task_aoi_split_retry(task_id: str):
    with EE_TASK_MONITORING_QUEUE_LOCK:
        aoi_coords = EE_TASK_MONITORING_QUEUE[task_id]['aoi_coords']
        file_name = EE_TASK_MONITORING_QUEUE[task_id]['file_name']
        attempt = EE_TASK_MONITORING_QUEUE[task_id]['attempt'] + 1
        del EE_TASK_MONITORING_QUEUE[task_id]

    if attempt > 100:
        print(f'Task[{task_id}] failed {attempt} times, aborting')
        return

    xmin = aoi_coords['xmin']
    ymin = aoi_coords['ymin']
    xmax = aoi_coords['xmax']
    ymax = aoi_coords['ymax']

    num_rows = SPLIT_BY
    num_cols = SPLIT_BY

    dx = (xmax - xmin) / num_cols
    dy = (ymax - ymin) / num_rows

    index = 0
    for row in range(num_rows):
        for col in range(num_cols):
            x0 = xmin + (dx * col)
            y0 = ymin + (dy * row)
            x1 = x0 + dx
            y1 = y0 + dy
            aoi = ee.Geometry.Rectangle([x0, y0, x1, y1])
            ccdc_input = ccdc_image_collection_preprocess(aoi)
            ccdc_result = ccdc(ccdc_input, aoi)
            ccdc_result_flat = ccdc_result_flaten(ccdc_result)
            file_name_cut = f'{file_name}_{index}'
            ccdc_result_export(ccdc_result_flat, aoi, file_name_cut, attempt)
            index += 1


def ee_task_simply_retry(task_id: str):
    with EE_TASK_MONITORING_QUEUE_LOCK:
        aoi_coords = EE_TASK_MONITORING_QUEUE[task_id]['aoi_coords']
        file_name = EE_TASK_MONITORING_QUEUE[task_id]['file_name']
        attempt = EE_TASK_MONITORING_QUEUE[task_id]['attempt'] + 1
        del EE_TASK_MONITORING_QUEUE[task_id]

    if attempt > 100:
        print(f'Task[{task_id}] failed {attempt} times, aborting')
        return

    xmin = aoi_coords['xmin']
    ymin = aoi_coords['ymin']
    xmax = aoi_coords['xmax']
    ymax = aoi_coords['ymax']

    aoi = ee.Geometry.Polygon([[
        [xmin, ymin],
        [xmax, ymin],
        [xmax, ymax],
        [xmin, ymax],
        [xmin, ymin]
    ]])
    ccdc_input = ccdc_image_collection_preprocess(aoi)
    ccdc_result = ccdc(ccdc_input, aoi)
    ccdc_result_flat = ccdc_result_flaten(ccdc_result)
    ccdc_result_export(ccdc_result_flat, aoi, file_name, attempt)


def ee_task_monitor():
    waits_empty_times = 0
    while True:
        if len(EE_TASK_QUEUE) == 0 and len(EE_TASK_MONITORING_QUEUE) == 0:
            waits_empty_times += 1
            if waits_empty_times > 10:
                break
            else:
                time.sleep(30)
                continue
        elif len(EE_TASK_QUEUE) > 0 and len(EE_TASK_MONITORING_QUEUE) < MAX_PARALLEL_TASKS:
            start_one_task()
        else:
            waits_empty_times = 0

        for task_id in list(EE_TASK_MONITORING_QUEUE.keys()):
            task = ee.batch.Task(
                task_id, EE_TASK_MONITORING_QUEUE[task_id]['type'], EE_TASK_MONITORING_QUEUE[task_id]['state']
            )
            try:
                task_status = task.status()
            except Exception as e:
                print(f'Task {task_id} failed to get status: {e}')
                time.sleep(30)
                continue
            if task_status['state'] == 'COMPLETED':
                print(f'Task {task_id} completed')
                with EE_TASK_MONITORING_QUEUE_LOCK:
                    del EE_TASK_MONITORING_QUEUE[task_id]
            elif task_status['state'] == 'FAILED':
                print(f'Task {task_id} failed')
                if task_status['error_message'] == 'User memory limit exceeded.':
                    print(f'{task_id} Error: User memory limit exceeded, attempt to split.')
                    ee_task_aoi_split_retry(task_id)
                elif task_status['error_message'] == 'Execution failed; out of memory.':
                    print(f'{task_id} Error: Execution failed, attempt to retry.')
                    ee_task_simply_retry(task_id)
                else:
                    print(f'Task {task_id} Error: "{task_status["error_message"]}", attempt to skip.')
                    with EE_TASK_MONITORING_QUEUE_LOCK:
                        del EE_TASK_MONITORING_QUEUE[task_id]
            elif CANCLE_TASK_TO_SPLIT and \
                    (task_status['state'] == 'CANCELLED' or task_status['state'] == 'CANCEL_REQUESTED'):
                print(f'Task {task_id} cancelled, try to split aoi')
                ee_task_aoi_split_retry(task_id)
            elif task_status['state'] == 'CANCELLED' or task_status['state'] == 'CANCEL_REQUESTED':
                print(f'Task {task_id} cancelled')
                with EE_TASK_MONITORING_QUEUE_LOCK:
                    del EE_TASK_MONITORING_QUEUE[task_id]
            time.sleep(30)


if __name__ == '__main__':
    task_monitor_thread = threading.Thread(target=ee_task_monitor)
    task_monitor_thread.start()
    ccdc_main()
