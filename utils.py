"""
utils.py
A collection of tools for working with data for GEE.
Author: Luhao Yang
Date: 2025-01-09
"""
import ee
import geemap
import datetime


def ee_init():
    ee.Authenticate()
    ee.Initialize(project="ee-yangluhao990714")


def ndsi(self: ee.Image) -> ee.Image:
    ndsi = self.normalizedDifference(['Green', 'SWIR1']).rename('NDSI')
    return self.addBands(ndsi)


ee.Image.ndsi = ndsi


def ndwi(self: ee.Image) -> ee.Image:
    ndwi = self.normalizedDifference(['Green', 'NIR']).rename('NDWI')
    return self.addBands(ndwi)


ee.Image.ndwi = ndwi


def ndvi(self: ee.Image) -> ee.Image:
    ndvi = self.normalizedDifference(['NIR', 'Red']).rename('NDVI')
    return self.addBands(ndvi)


ee.Image.ndvi = ndvi


def evi(self: ee.Image) -> ee.Image:
    evi = self.expression('2.5 * ((NIR - Red) / (NIR + 6 * Red - 7.5 * Blue + 1))', {
        'NIR': self.select('NIR'),
        'Red': self.select('Red'),
        'Blue': self.select('Blue')
    }).rename('EVI')
    return self.addBands(evi)


ee.Image.evi = evi


def savi(self) -> ee.Image:
    savi = self.expression('1.5 * ((NIR - Red) / (NIR + Red + 0.5))', {
        'NIR': self.select('NIR'),
        'Red': self.select('Red')
    }).rename('SAVI')
    return self.addBands(savi)


ee.Image.savi = savi


def nbr(self) -> ee.Image:
    nbr = self.normalizedDifference(['NIR', 'SWIR2']).rename('NBR')
    return self.addBands(nbr)


ee.Image.nbr = nbr


def kt_transform(self: ee.Image) -> ee.Image:
    '''
    Calculate the brightness, greenness, and wetness using the Kauth-Thomas transform.

    Args:
    self: ee.Image - The input image

    Returns:
    ee.Image - The image with the brightness, greenness, and wetness bands added

    Reference:
    [1] R. Nedkov, “ORTHOGONAL TRANSFORMATION OF SEGMENTED IMAGES FROM THE SATELLITE SENTINEL-2,” 2017.
    '''
    brightness = self.expression(
        '0.0356 * B1 + 0.0822 * B2 + 0.1360 * B3 + 0.2611 * B4 + 0.2964 * B5 + 0.3338 * B6 + 0.3877 * B7 + 0.3895 * B8 \
            + 0.0949 * B9 + 0.0009 * B10 + 0.3882 * B11 + 0.1366 * B12 + 0.4750 * B8A',
        {
            'B1': self.select('Aerosol'),
            'B2': self.select('Blue'),
            'B3': self.select('Green'),
            'B4': self.select('Red'),
            'B5': self.select('RedEdge1'),
            'B6': self.select('RedEdge2'),
            'B7': self.select('RedEdge3'),
            'B8': self.select('NIR'),
            'B8A': self.select('RedEdge4'),
            'B9': self.select('WaterVapor'),
            'B10': self.select('Cirrus'),
            'B11': self.select('SWIR1'),
            'B12': self.select('SWIR2')
        }
    ).rename('TCB')
    greenness = self.expression(
        '-0.0635 * B1 - 0.1128 * B2 - 0.1680 * B3 - 0.3480 * B4 - 0.3303 * B5 + 0.0852 * B6 + 0.3302 * B7 + 0.3165 * \
            B8 + 0.0467 * B9 - 0.0009 * B10 - 0.4578 * B11 - 0.4064 * B12 + 0.3625 * B8A',
        {
            'B1': self.select('Aerosol'),
            'B2': self.select('Blue'),
            'B3': self.select('Green'),
            'B4': self.select('Red'),
            'B5': self.select('RedEdge1'),
            'B6': self.select('RedEdge2'),
            'B7': self.select('RedEdge3'),
            'B8': self.select('NIR'),
            'B8A': self.select('RedEdge4'),
            'B9': self.select('WaterVapor'),
            'B10': self.select('Cirrus'),
            'B11': self.select('SWIR1'),
            'B12': self.select('SWIR2')
        }
    ).rename('TCG')
    wetness = self.expression(
        '0.0649 * B1 + 0.1363 * B2 + 0.2802 * B3 + 0.3072 * B4 + 0.5288 * B5 + 0.1379 * B6 - 0.0001 * B7 - 0.0807 * B8 \
            - 0.0302 * B9 + 0.0003 * B10 - 0.4064 * B11 - 0.5602 * B12 - 0.1389 * B8A',
        {
            'B1': self.select('Aerosol'),
            'B2': self.select('Blue'),
            'B3': self.select('Green'),
            'B4': self.select('Red'),
            'B5': self.select('RedEdge1'),
            'B6': self.select('RedEdge2'),
            'B7': self.select('RedEdge3'),
            'B8': self.select('NIR'),
            'B8A': self.select('RedEdge4'),
            'B9': self.select('WaterVapor'),
            'B10': self.select('Cirrus'),
            'B11': self.select('SWIR1'),
            'B12': self.select('SWIR2')
        }
    ).rename('TCW')
    return self.addBands(brightness).addBands(greenness).addBands(wetness)


ee.Image.kt_transform = kt_transform


def split_region(region: ee.Geometry, num_tiles) -> list:
    coords = region.bounds().coordinates().get(0).getInfo()
    min_lon, min_lat = coords[0]
    max_lon, max_lat = coords[2]
    lon_step = (max_lon - min_lon) / num_tiles
    lat_step = (max_lat - min_lat) / num_tiles

    tiles = []
    for i in range(num_tiles):
        for j in range(num_tiles):
            tile = ee.Geometry.Rectangle([
                min_lon + i * lon_step,
                min_lat + j * lat_step,
                min_lon + (i + 1) * lon_step,
                min_lat + (j + 1) * lat_step
            ])
            tiles.append(tile)
    return tiles


def _sentinel_2_msi_multispectral_instrument_level_2a_band_rename(image: ee.Image):
    band_rename_dic = ee.Dictionary({
        'B1': 'Aerosol',
        'B2': 'Blue',
        'B3': 'Green',
        'B4': 'Red',
        'B5': 'RedEdge1',
        'B6': 'RedEdge2',
        'B7': 'RedEdge3',
        'B8': 'NIR',
        'B8A': 'RedEdge4',
        'B9': 'WaterVapor',
        'B10': 'Cirrus',
        'B11': 'SWIR1',
        'B12': 'SWIR2',
        'QA60': 'QA',
    })
    return image.select(band_rename_dic.keys()).rename(band_rename_dic.values())


def _sentinel_2_msi_multispectral_instrument_level_1c_band_rename(image: ee.Image):
    band_rename_dic = ee.Dictionary({
        'B1': 'Aerosol',
        'B2': 'Blue',
        'B3': 'Green',
        'B4': 'Red',
        'B5': 'RedEdge1',
        'B6': 'RedEdge2',
        'B7': 'RedEdge3',
        'B8': 'NIR',
        'B8A': 'RedEdge4',
        'B9': 'WaterVapor',
        'B10': 'Cirrus',
        'B11': 'SWIR1',
        'B12': 'SWIR2',
        'QA60': 'QA',
    })
    return image.select(band_rename_dic.keys()).rename(band_rename_dic.values())


def band_rename(self) -> ee.ImageCollection:
    """
    Rename bands of the input image collection.

    Args:
    Return:
    ee.ImageCollection
    """
    collection_title = self.getInfo()['properties']['title']
    match collection_title:
        case 'Sentinel-2 MSI: MultiSpectral Instrument, Level-2A':
            self = self.map(lambda img: _sentinel_2_msi_multispectral_instrument_level_2a_band_rename(img))
        case 'Sentinel-2 MSI: MultiSpectral Instrument, Level-1C':
            self = self.map(lambda img: img.addBands(
                _sentinel_2_msi_multispectral_instrument_level_1c_band_rename(img)
            ))
        case _:
            print(f'The input image collection [{collection_title}] is not supported.')
    return self


ee.ImageCollection.band_rename = band_rename


def remove_clouds(self) -> ee.ImageCollection:
    """
    Remove clouds from the input image collection.

    Args:
    Return:
    ee.ImageCollection
    """
    collection_title = self.getInfo()['properties']['title']
    match collection_title:
        case 'Sentinel-2 MSI: MultiSpectral Instrument, Level-2A':
            self = self.map(lambda img: ee.Algorithms.If(img.date().millis().gte(ee.Date('2022-01-25').millis()).Or(
                img.date().millis().gte(ee.Date('2024-02-28').millis())),
                img.updateMask(img.select('CLD_PRB').lt(40)),
                img.updateMask(img.select('QA').bitwiseAnd(1 << 10).eq(0).And(
                    img.select('QA').bitwiseAnd(1 << 11).eq(0)))
                )
            )
        case 'Sentinel-2 MSI: MultiSpectral Instrument, Level-1C':
            self = self.map(lambda img: img.updateMask(ee.Algorithms.Sentinel2.CDI(img)))
        case _:
            print(f'The input image collection [{collection_title}] is not supported.')
    return self


ee.ImageCollection.remove_clouds = remove_clouds


def quarterly_composite(self, start_date: ee.Date, end_date: ee.Date) -> ee.ImageCollection:
    """
    Generate quarterly composites from the input image collection.

    Args:
    start_date: ee.Date
    end_date: ee.Date
    Return:
    ee.ImageCollection
    """
    quarters = ee.List.sequence(1, 4)
    years = ee.List.sequence(start_date.get('year'), end_date.get('year'))

    def _single_quarter_composite(year, quarter):
        quarter = ee.Number(quarter)
        start_month = quarter.multiply(3).subtract(2)
        start = ee.Date.fromYMD(year, start_month, 1)
        end = start.advance(3, 'month')
        filtered = self.filterDate(start, end)
        composite = ee.Algorithms.If(filtered.size().gt(0),
            filtered.median().set('system:time_start', start.millis()), None)
        return ee.Image(composite)

    composites = years.map(lambda y: quarters.map(lambda q: _single_quarter_composite(y, q))).flatten()
    return ee.ImageCollection.fromImages(composites)


ee.ImageCollection.quarterly_composite = quarterly_composite


def monthly_composite(self, start_date: ee.Date, end_date: ee.Date) -> ee.ImageCollection:
    """
    Generate monthly composites from the input image collection.

    Args:
    start_date: ee.Date
    end_date: ee.Date
    Return:
    ee.ImageCollection
    """
    months = ee.List.sequence(1, 12)
    years = ee.List.sequence(start_date.get('year'), end_date.get('year'))

    def _single_month_composite(year, month):
        start = ee.Date.fromYMD(year, month, 1)
        end = start.advance(1, 'month')
        filtered = self.filterDate(start, end)
        composite = ee.Algorithms.If(
            filtered.size().gt(0),
            filtered.mean().set('system:time_start',
            start.millis()),
            None
        )
        return ee.Image(composite)

    composites = years.map(lambda y: months.map(lambda m: _single_month_composite(y, m))).flatten()
    return ee.ImageCollection.fromImages(composites)


ee.ImageCollection.monthly_composite = monthly_composite


def annual_composite(self, start_date: ee.Date, end_date: ee.Date) -> ee.ImageCollection:
    """
    Generate annual composites from the input image collection.

    Args:
    start_date: ee.Date
    end_date: ee.Date
    Return:
    ee.ImageCollection
    """
    years = ee.List.sequence(start_date.get('year'), end_date.get('year'))

    def _single_year_composite(year):
        start = ee.Date.fromYMD(year, 1, 1)
        end = start.advance(1, 'year')
        filtered = self.filterDate(start, end)
        composite = ee.Algorithms.If(filtered.size().gt(0),
            filtered.median().set('system:time_start', start.millis()),
            None
        )
        return ee.Image(composite)

    composites = years.map(lambda y: _single_year_composite(y))
    return ee.ImageCollection.fromImages(composites)


ee.ImageCollection.annual_composite = annual_composite


def temporal_composite(self, start_date: ee.Date, end_date: ee.Date, temporal_resolution: str) -> ee.ImageCollection:
    """
    Generate temporal composites from the input image collection.

    Args:
    start_date: ee.Date
    end_date: ee.Date
    temporal_resolution: str ('quarterly', 'monthly', 'annual')
    Return:
    ee.ImageCollection
    """
    match temporal_resolution:
        case 'quarterly':
            self = self.quarterly_composite(start_date, end_date)
        case 'monthly':
            self = self.monthly_composite(start_date, end_date)
        case 'annual':
            self = self.annual_composite(start_date, end_date)
    return self


ee.ImageCollection.temporal_composite = temporal_composite


def year_to_millis(year: float) -> int:
    """
    Convert a decimal year to milliseconds since the epoch.

    Args:
    year: float - The decimal year (e.g., 2023.17)

    Returns:
    int - The corresponding milliseconds since the epoch
    """
    # 提取年份和小数部分
    year_int = int(year)
    decimal_part = year - year_int

    # 计算该年份的总天数
    start_of_year = datetime.datetime(year_int, 1, 1)
    end_of_year = datetime.datetime(year_int + 1, 1, 1)
    microseconds_in_year = (end_of_year - start_of_year).microseconds

    # 计算小数部分对应的天数
    microseconds = decimal_part * microseconds_in_year

    # 计算目标日期
    target_date = start_of_year + datetime.timedelta(microseconds=microseconds)

    # 将目标日期转换为毫秒
    millis = int(target_date.timestamp() * 1000)
    return millis


def millis_to_date(millis: int) -> str:
    """
    Convert milliseconds since the epoch to a date string.

    Args:
    millis: int - The milliseconds since the epoch

    Returns:
    str - The corresponding date string
    """
    date = datetime.datetime.fromtimestamp(millis / 1000)
    return date.strftime('%Y-%m-%d')


def create_grid_aoi(aoi: ee.Geometry, interval: int) -> ee.FeatureCollection:
    """
    Create a grid of AOIs from the input AOI.

    Args:
    aoi: ee.Geometry - The input AOI
    interval: int - The interval of the grid

    Returns:
    ee.FeatureCollection - The grid of AOIs
    """
    bounds = aoi.bounds()
    coords = ee.List(bounds.coordinates().get(0))

    xmin = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.min())
    ymin = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.min())
    xmax = coords.map(lambda p: ee.Number(ee.List(p).get(0))).reduce(ee.Reducer.max())
    ymax = coords.map(lambda p: ee.Number(ee.List(p).get(1))).reduce(ee.Reducer.max())

    x_steps = xmax.subtract(xmin).divide(interval).ceil().int()
    y_steps = ymax.subtract(ymin).divide(interval).ceil().int()

    def make_grid(x_idx, y_idx):
        x0 = xmin.add(ee.Number(x_idx).multiply(interval))
        y0 = ymin.add(ee.Number(y_idx).multiply(interval))
        x1 = x0.add(interval)
        y1 = y0.add(interval)
        rect = ee.Geometry.Rectangle([x0, y0, x1, y1])
        return ee.Feature(rect)

    full_grid = ee.FeatureCollection([
        make_grid(x, y) for x in range(x_steps.getInfo()) for y in range(y_steps.getInfo())
    ])

    grid_filtered = full_grid.filterBounds(aoi)

    return grid_filtered