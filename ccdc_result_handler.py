import ee
import threading
import utils
import time


class _HandlerThread(threading.Thread):
    ccdc_res: ee.ImageCollection
    ccdc_res_list: list
    res_list_lock = threading.Lock()
    out_path: str
    out_path_exists_list: list
    bands_basename = [
        'tBreak', 'Blue_magnitude', 'Green_magnitude', 'Red_magnitude', 'NIR_magnitude', 'SWIR1_magnitude',
        'SWIR2_magnitude', 'NDVI_magnitude', 'EVI_magnitude', 'TCB_magnitude', 'TCG_magnitude', 'TCW_magnitude',
        'changeProb'
    ]
    bands_names: dict[str, list[str]]
    max_threads: int = 1
    base_band_len: int = 10
    change_prob_threshold: float = 0.95
    start_time: time.struct_time
    end_time: time.struct_time
    min_patch_size: int = 10

    def __init__(self):
        super().__init__()
        pass

    def _is_empty(self) -> bool:
        with _HandlerThread.res_list_lock:
            return self.ccdc_res_list == []

    def _patch_cal(self, image: ee.Image) -> ee.Image:
        image_int = ee.Image(image).multiply(1000).toInt32()
        labels = image_int.connectedComponents(
            connectedness=ee.Kernel.circle(50),
            maxSize=1024,
        ).select(['labels'])
        patch_size = labels.connectedPixelCount(
            maxSize=1024,
            eightConnected=False,
        )
        image = ee.Image(image).updateMask(patch_size.gte(self.min_patch_size))
        return image

    @staticmethod
    def _get_image_interval(bands: dict, year: int) -> ee.Image:
        start_year = year
        end_year = year + 1
        time_mask = bands['tBreak'].gte(start_year).And(bands['tBreak'].lt(end_year))

        def extract_band(image: ee.Image) -> ee.Image:
            def extract_band_inner(image: ee.Image, band_name) -> ee.Image:
                return image.select(band_name).rename('band')

            image_extracted = image.bandNames().map(lambda band_name: extract_band_inner(image, ee.String(band_name)))
            image_collection = ee.ImageCollection(image_extracted)
            image_combine = image_collection.median()
            return image_combine

        res_list = []
        for k, v in bands.items():
            res_list.append(extract_band(ee.Image(v).updateMask(time_mask)).rename(f'{k}_{start_year}'))
        res = ee.Image(res_list[0])
        for it in res_list[1:]:
            res = res.addBands(ee.Image(it))
        return res

    def _run_inner(self, image: ee.Image, image_name: str) -> None:
        start_time = self.start_time.tm_year
        end_time = self.end_time.tm_year
        bounds = image.geometry().bounds()
        change_prob = image.select(self.bands_names['changeProb'])
        t_break = image.select(self.bands_names['tBreak'])
        prob_mask = change_prob.gte(self.change_prob_threshold)
        names_without_change_prob = self.bands_basename.copy()
        names_without_change_prob.remove('changeProb')
        masked_bands = {
            key: image.select(self.bands_names[key]).updateMask(prob_mask)
            for key in names_without_change_prob
        }
        for year in range(start_time, end_time + 1):
            file_name = f'{image_name}_{year}'
            asset_id =f'{self.out_path}/{file_name}'
            if file_name in self.out_path_exists_list:
                continue
            cur_image = self._get_image_interval(masked_bands, year)
            cur_image = self._patch_cal(cur_image)
            while True:
                task = ee.batch.Export.image.toAsset(
                    image=cur_image,
                    description='export_' + file_name,
                    assetId= asset_id,
                    scale=10,
                    maxPixels=1e13,
                    region=bounds,
                    crs='EPSG:4326',
                )
                if utils.start_task_and_monitoring(task):
                    break

    def run(self):
        while not self._is_empty():
            with self.res_list_lock:
                res = self.ccdc_res_list.pop(0)
            image = ee.Image(res['name'])
            self._run_inner(image, res['name'].split('/')[-1])

    @classmethod
    def set_attribute(cls, ccdc_res_path: str = None, out_path: str = None, max_threads: int = 1,
                      change_prob_threshold: float = 0.95, **kwargs):
        """
        Args:
            ccdc_res_path (str):
            out_path (str):
            max_threads (int):
            change_prob_threshold (float):
            **kwargs:

        Keyword Args:
            start_time (str):
            end_time (str):
            time_format (str):
            change_prob_threshold (int):
            min_patch_size (int):
        """
        if ccdc_res_path:
            cls.ccdc_res = ee.ImageCollection(ccdc_res_path)
        if out_path:
            cls.out_path = out_path
        if max_threads:
            cls.max_threads = max_threads
        cls.ccdc_res_list = ee.data.listAssets(ccdc_res_path)['assets']
        cls.out_path_exists_list = [item['name'].split('/')[-1] for item in ee.data.listAssets(out_path)['assets']]
        cls.change_prob_threshold = change_prob_threshold
        if kwargs:
            if 'start_time' in kwargs and 'time_format' in kwargs:
                cls.start_time = time.strptime(kwargs['start_time'], kwargs['time_format'])
            if 'end_time' in kwargs and 'time_format' in kwargs:
                cls.end_time = time.strptime(kwargs['end_time'], kwargs['time_format'])
            if 'change_prob_threshold' in kwargs:
                cls.change_prob_threshold = kwargs['change_prob_threshold']
            if 'min_patch_size' in kwargs:
                cls.min_patch_size = kwargs['min_patch_size']

    @classmethod
    def run_all(cls):
        cls.bands_names = {}
        for basename in cls.bands_basename:
            cls.bands_names[basename] = []
        for basename in cls.bands_basename:
            for index in range(cls.base_band_len):
                cls.bands_names[basename].append(f'{basename}_{index}')
        threads = []

        def spawn_one():
            t = _HandlerThread()
            t.daemon = True
            t.start()
            threads.append(t)

        for _ in range(cls.max_threads):
            if not cls.ccdc_res_list:
                break
            spawn_one()
        while True:
            threads = [t for t in threads if t.is_alive()]
            while cls.ccdc_res_list and len(threads) < cls.max_threads:
                spawn_one()
            if not cls.ccdc_res_list and not any(t.is_alive() for t in threads):
                break
            time.sleep(0.5)
        for t in threads:
            t.join(timeout=0.1)


def _mosiac(out_path: str, tmp_path: str, aoi_path: str, start_year: int, end_year: int) -> None:
    ic = ee.ImageCollection(tmp_path)
    res_exists_l = [item['name'].split('/')[-1] for item in ee.data.listAssets(out_path)['assets']]
    if aoi_path:
        aoi = ee.FeatureCollection(aoi_path).geometry()
    else:
        aoi = ic.geometry().bounds()
    try:
        existing = ee.data.listAssets(out_path).get('assets', [])
        existing_names = {item['name'].split('/')[-1] for item in existing}
    except Exception:
        existing_names = set()
    for year in range(start_year, end_year + 1):
        file_name = f'ccdc_result_{year}'
        if file_name in existing_names:
            continue
        subset = ic.filter(ee.Filter.stringEndsWith('system:index', f'_{year}')).sort('system:index')
        if ee.Number(subset.size()).eq(0).getInfo():
            continue
        img = subset.mosaic().set({'year': year})
        asset_id = f'{out_path}{file_name}' if out_path.endswith('/') else f'{out_path}/{file_name}'
        if file_name in res_exists_l:
            continue
        while True:
            task = ee.batch.Export.image.toAsset(
                image=img,
                description='export_' + file_name,
                assetId=asset_id,
                scale=10,
                maxPixels=1e13,
                region=aoi,
                crs='EPSG:4326',
            )
            if utils.start_task_and_monitoring(task):
                break


def ccdc_result_handler(res_path: str, out_path: str, tmp_path: str = None, aoi_path: str = None,
                        max_threads: int = 1, start_year: int = None, end_year: int = None, overwrite = False) -> None:
    """Handle with CCDC result.

    This method will create max_thread threads to process each CCDC result and temporarily store the outputs in the
    tmp_path directory. Finally, the results will be mosaicked into a single image and saved to out_path. The tmp_path
    directory will be automatically created and deleted within the function to ensure it is empty.

    Args:
        res_path (str): Path to the CCDC result directory or image collection.
        out_path (str): Path to the output directory.
        tmp_path (str): Path to the temporary directory or image collection, will be automatically created and deleted
            within. Defaults to an image collection named tmp under your out_path.
        max_threads (int): Maximum number of threads to process each CCDC result and temporarily store the output.
            Defaults to 1.
        aoi_path (str): Path to the area of interest. Defaults to None. If it's None, won't clip.
        start_year (int):
        end_year (int):
    """
    if tmp_path is None:
        tmp_path = rf'{out_path}tmp' if out_path.endswith('/') else rf'{out_path}/tmp'
    if overwrite:
        utils.create_ee_image_collection_with_overwrite(tmp_path)
    else :
        utils.create_ee_image_collection(tmp_path)
    _HandlerThread.set_attribute(res_path, tmp_path, max_threads, start_time=f'{start_year}', end_time=f'{end_year}',
                                 time_format='%Y', )
    _HandlerThread.run_all()
    _mosiac(out_path, tmp_path, aoi_path, start_year, end_year)
    # utils.del_ee_image_collection(tmp_path)


if __name__ == '__main__':
    utils.ee_init()
    ccdc_result_handler(
        res_path=r'path',
        out_path=r'path',
        aoi_path=r'path',
        max_threads=4,
        start_year=2015,
        end_year=2025,
    )
