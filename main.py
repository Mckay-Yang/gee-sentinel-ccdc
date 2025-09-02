import ee
import threading
import time
from typing import Optional
import os
import inspect
import datetime
from ccdc_result_handler import ccdc_result_handler

ee.Authenticate()
ee.Initialize(project='project-id')

EE_TASK_MONITORING_QUEUE: dict[dict: dict] = {}
EE_TASK_MONITORING_QUEUE_LOCK = threading.Lock()
EE_TASK_QUEUE: list[dict] = []
EE_TASK_QUEUE_LOCK = threading.Lock()

band_groups = {
    'tBreak': False,
    'changeProb': False,
    'Blue': True,
    'Green': True,
    'Red': True,
    'NIR': True,
    'SWIR1': True,
    'SWIR2': True,
}

def expand_band(name: str, magnitude: bool = False, n: int = 10):
    if magnitude:
        return [f'{name}_magnitude_{i}' for i in range(n)]
    else:
        return [f'{name}_{i}' for i in range(n)]

BAND_LIST = ee.List([
    ee.Dictionary({name if not mag else f'{name}_magnitude': expand_band(name, mag)})
    for name, mag in band_groups.items()
])

START_DATE = ee.Date('2015-06-27')
END_DATE = ee.Date('2025-08-21')
AOI_GRID = ee.FeatureCollection('projects/project-id/assets/AOIs/aoi')
TP_FOREST_MASK: ee.Image = ee.Image('').select(['b1']).neq(0)
COLLECTION_TITLE = 'COPERNICUS/S2_HARMONIZED'
IMAGE_COLLECTION = ee.ImageCollection(COLLECTION_TITLE)
MAX_PARALLEL_TASKS = 10
CANCLE_TASK_TO_SPLIT = True
OUTPUT_COLLECTION = 'CCDC/ccdc_raw/'
SPLIT_BY = 2
ASSETS_PATH = ''


OUTPUT_COLLECTION = OUTPUT_COLLECTION if OUTPUT_COLLECTION.endswith('/') else OUTPUT_COLLECTION + '/'
ASSETS_PATH = ASSETS_PATH if ASSETS_PATH.endswith('/') else ASSETS_PATH + '/'


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
        EE_TASK_QUEUE.append({'task': task, 'aoi_coords': {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax},
                              'file_name': file_name, 'attempt': attempt, })


def get_ee_task_queue() -> Optional[dict]:
    if len(EE_TASK_QUEUE) == 0:
        return None
    with EE_TASK_QUEUE_LOCK:
        return EE_TASK_QUEUE.pop(0)


def append_ee_task_monitoring_queue(task: ee.batch.Task, aoi_coords: ee.List, file_name: str, attempt: int):
    with EE_TASK_MONITORING_QUEUE_LOCK:
        EE_TASK_MONITORING_QUEUE[task.id] = {  # To cut current aoi into smaller pieces
            'aoi_coords': aoi_coords,
            # To get the task info, the three fields are necessary
            'id': task.id,
            'state': ee.batch.Task.State(task.status()['state']),
            'type': ee.batch.Task.Type(task.status()['task_type']),
            'file_name': file_name,
            'attempt': attempt, }


def ccdc_image_collection_preprocess(aoi: ee.Geometry) -> ee.ImageCollection:
    img_col = IMAGE_COLLECTION.filterBounds(aoi).filterDate(START_DATE, END_DATE)
    img_col = img_col.remove_clouds(COLLECTION_TITLE)
    img_col = img_col.band_rename(COLLECTION_TITLE)
    img_col = img_col.map(lambda img: img.updateMask(
        img.ndsi().select('NDSI').lt(0).And(img.ndwi().select('NDWI').lt(0))))
    ret = img_col.select(['Blue', 'Green', 'Red', 'NIR', 'SWIR1', 'SWIR2'])
    return ret


def ccdc(ccdc_input: ee.ImageCollection, aoi: ee.Geometry) -> ee.Image:
    ccdc_result: ee.Image = ee.Algorithms.TemporalSegmentation.Ccdc(
        ccdc_input,
        minObservations=18,
        dateFormat=1,
        chiSquareProbability=0.999,
        maxIterations=25000,
    )
    return ccdc_result


def ccdc_result_flaten(ccdc_result: ee.Image) -> ee.Image:
    ccdc_result_flat_list = BAND_LIST.map(
        lambda band: ccdc_result.select([ee.Dictionary(band).keys().get(0)]).arrayPad([10], 0) \
            .arrayFlatten([ee.Dictionary(band).values().get(0)])
    )
    ccdc_result_flat = ee.Image(ccdc_result_flat_list.get(0)) \
        .addBands(ee.Image(ccdc_result_flat_list.get(1))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(2))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(3))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(4))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(5))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(6))) \
        .addBands(ee.Image(ccdc_result_flat_list.get(7)))
    return ccdc_result_flat


def ccdc_result_export(ccdc_result_flat: ee.Image, aoi: ee.Geometry, file_name: str, attempt: int = 1):
    task = ee.batch.Export.image.toAsset(
        image=ccdc_result_flat.clip(aoi),
        description='export_' + file_name,
        assetId=f'{ASSETS_PATH}{OUTPUT_COLLECTION}{file_name}',
        scale=10,
        region=aoi,
        maxPixels=1e13,
        crs='EPSG:4326',
    )
    append_ee_task_queue(task, aoi, file_name, attempt)


def start_one_task():
    task_dict = get_ee_task_queue()
    if task_dict is not None:
        task_dict['task'].start()
        append_ee_task_monitoring_queue(task_dict['task'], task_dict['aoi_coords'], task_dict['file_name'],
                                        task_dict['attempt'])
        print('Task', task_dict['task'].id, 'started')


def ccdc_main():
    index = 0
    for aoi_grid_feature in AOI_GRID.getInfo()['features']:
        aoi = ee.Feature(aoi_grid_feature['geometry']).geometry()
        ccdc_input = ccdc_image_collection_preprocess(aoi)
        ccdc_result = ccdc(ccdc_input, aoi)
        ccdc_result_flat = ccdc_result_flaten(ccdc_result)
        file_name = f'ccdc_result_{index}'
        ccdc_result_export(ccdc_result_flat, aoi, file_name)
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

    aoi = ee.Geometry.Polygon([[[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax], [xmin, ymin]]])
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
            task = ee.batch.Task(task_id, EE_TASK_MONITORING_QUEUE[task_id]['type'],
                                 EE_TASK_MONITORING_QUEUE[task_id]['state'])
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
            elif CANCLE_TASK_TO_SPLIT and (
                    task_status['state'] == 'CANCELLED' or task_status['state'] == 'CANCEL_REQUESTED'):
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
    ccdc_result_handler(res_path='projects/project_id/assets/CCDC/ccdc_raw',
        out_path='users/yangluhao990714/ccdc_results/ccdc_5th',
        tmp_path='projects/project_id/assets/CCDC/final_18_0999_tmp', aoi_path='projects/project_id/assets/AOIs/aoi',
        max_threads=8, start_year=2015, end_year=2025, )
