from io import BytesIO
import matplotlib.image as mpimg
import numpy as np
import pandas as pd
import aiohttp
import asyncio
import async_timeout
from scipy.sparse import coo_matrix
import requests
from concurrent.futures import ThreadPoolExecutor


def create_img_df(img: coo_matrix, ds_ind: int, ion_ind: int, formula_ind: int, fdr: int):
    img_df = pd.DataFrame()
    # non_zero_pix_ind = img.toarray().ravel() > 0
    img_df['x'] = img.col.astype(np.int16)
    img_df['y'] = img.row.astype(np.int16)
    img_df['int'] = img.data.astype(np.float32)
    img_df['ds_ind'] = np.int16(ds_ind)
    img_df['ion_ind'] = np.int32(ion_ind)
    img_df['formula_ind'] = np.int32(formula_ind)
    img_df['fdr'] = np.int8(fdr)
    return img_df


# Asyncio

# async def fetch_iso_img(session, url):
#     with async_timeout.timeout(10):
#         async with session.get(url) as response:
#             img_bytes = await response.read()
#             img = mpimg.imread(BytesIO(img_bytes))
#             data = img[:, :, 0]
#             mask = img[:, :, -1]
#             data[mask == 0] = 0
#             return coo_matrix(data)
#
#


# async def main(image_api_endpoint, ds_ind, anns, ion_inv_ind, formula_inv_ind):
#     async with aiohttp.ClientSession() as session:
#         results = []
#         for ann in anns:
#             url = image_api_endpoint + ann['isotopeImages'][0]['url']
#             img = await fetch_iso_img(session, url)
#             ion_ind = ion_inv_ind[(ann['sumFormula'], ann['adduct'])]
#             formula_ind = formula_inv_ind[ann['sumFormula']]
#             img_df = await create_img_df(img, ds_ind, ion_ind, formula_ind, ann['fdrLevel'] * 100)
#             results.append(img_df)
#         return results


# def create_all_img_df(image_api_endpoint, ds_ind, anns, ion_inv_ind, formula_inv_ind):
#     loop = asyncio.get_event_loop()
#     future = main(image_api_endpoint, ds_ind, anns, ion_inv_ind, formula_inv_ind)
#     img_df_list = loop.run_until_complete(future)
#     return img_df_list


# Concurrent

def fetch_iso_img(url):
    img_bytes = requests.get(url).content
    img = mpimg.imread(BytesIO(img_bytes))
    data = img[:, :, 0]
    mask = img[:, :, -1]
    data[mask == 0] = 0
    return coo_matrix(data)


def create_ds_pixel_df(image_api_endpoint, ds_ind, anns, ion_inv_ind, formula_inv_ind):

    def fetch(ann):
        url = image_api_endpoint + ann['isotopeImages'][0]['url']
        img = fetch_iso_img(url)
        ion_ind = ion_inv_ind[(ann['sumFormula'], ann['adduct'])]
        formula_ind = formula_inv_ind[ann['sumFormula']]
        img_df = create_img_df(img, ds_ind, ion_ind, formula_ind, ann['fdrLevel'] * 100)
        return img_df

    with ThreadPoolExecutor() as executor:
        results = executor.map(fetch, anns)
    return results


def pack_ds_pixel_ann(image_api_endpoint, ds_ind, anns, path, ion_inv_ind, formula_inv_ind):
    path.mkdir(exist_ok=True)
    ds_pixel_df_list = create_ds_pixel_df(image_api_endpoint, ds_ind, anns, ion_inv_ind, formula_inv_ind)

    if ds_pixel_df_list:
        ds_pixel_df = pd.concat(ds_pixel_df_list).sort_values(by=['ds_ind', 'x', 'y'])
        ds_pixel_df.to_msgpack(path / f'{ds_ind}.msgpack', compress='zlib')
    else:
        print('No annotations to pack')
