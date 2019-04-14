from itertools import product
import pandas as pd

from pack_annotations import pack_ds_pixel_ann


def pack_moldb(sm_inst, moldb_name, data_path, adducts):
    db_df_path = data_path / 'db_df.msgpack'

    if db_df_path.exists():
        print(f'{moldb_name} molecular database already packed')
    else:
        print(f'Packing {moldb_name} molecular database with {adducts} adducts')

        moldb = sm_inst.database(name=moldb_name)
        molecules = moldb.molecules(limit=1000000)
        db_df = pd.DataFrame(molecules)
        db_df['db_id'] = moldb._id
        db_df.to_msgpack(db_df_path)

        ion_df = pd.DataFrame()
        formulas = db_df.sf.unique()
        ion_df['formula'], ion_df['adduct'] = zip(*product(formulas, adducts))
        formula_df = pd.DataFrame(formulas, columns=['formula'])

        ion_df.to_msgpack(data_path / 'ion_df.msgpack')
        formula_df.to_msgpack(data_path / 'formula_df.msgpack')


def fetch_ds_meta(ds_ids):
    return pd.DataFrame([{'ds_id': ds_id}
                         for ds_id in ds_ids])


def read_ds_df(ds_df_path):
    if ds_df_path.exists():
        ds_df = pd.read_msgpack(ds_df_path)
    else:
        ds_df = pd.DataFrame([], columns=['ds_id'])
    return ds_df


def read_inv_inds(data_path):
    ion_df = pd.read_msgpack(data_path / 'ion_df.msgpack')
    formula_df = pd.read_msgpack(data_path / 'formula_df.msgpack')
    ion_inv_ind = {(t.formula, t.adduct): t.Index for t in ion_df.itertuples()}
    formula_inv_ind = {t.formula: t.Index for t in formula_df.itertuples()}
    return ion_inv_ind, formula_inv_ind


def pack_datasets(image_api_endpoint, data_path, sm_inst,
                  ds_ids, moldb_name='HMDB-v4', fdr=0.5, overwrite=False):
    ds_ids = set(ds_ids)
    print(f'{len(ds_ids)} dataset ids provided')

    ds_df_path = data_path / 'ds_df.msgpack'
    ds_df = read_ds_df(ds_df_path)
    saved_ds_ids = set(ds_df.ds_id.unique())

    if overwrite:
        overwrite_ds_ids = saved_ds_ids & ds_ids
        print(f'{len(overwrite_ds_ids)} saved datasets will be overwritten: {overwrite_ds_ids}')
        ds_ids_to_pack = ds_ids
    else:
        print(f'Datasets already saved: {saved_ds_ids & ds_ids}')
        ds_ids_to_pack = ds_ids - saved_ds_ids

    if len(ds_ids_to_pack) > 0:
        ion_inv_ind, formula_inv_ind = read_inv_inds(data_path)

        new_ds_ids = ds_ids - saved_ds_ids
        ds_df_temp = fetch_ds_meta(new_ds_ids)
        ds_df = ds_df.append(ds_df_temp, ignore_index=True)

        for t in ds_df[ds_df.ds_id.isin(ds_ids_to_pack)].itertuples():
            try:
                ds_ind, ds_id = t.Index, t.ds_id
                print(f'Packing {ds_ind} {ds_id}')

                ds = sm_inst.dataset(id=ds_id)
                ann_fields = ('sumFormula', 'adduct', 'isotopeImages', 'fdrLevel')
                anns = [dict(zip(ann_fields, ann)) for ann in
                        ds.annotations(database=moldb_name, fdr=fdr, return_vals=ann_fields)]

                pack_ds_pixel_ann(image_api_endpoint, ds_ind, anns, data_path / 'pixel_df_list',
                                  ion_inv_ind, formula_inv_ind)
            except Exception as e:
                print(f'Failed to export {ds_id}: {e}')

        print(f'Packing dataset dataframe')
        ds_df.to_msgpack(ds_df_path)


DEFAULT_ADDUCTS = ('+H', '+Na', '+K', '-H', '+Cl')


def pack_metaspace(image_api_endpoint, data_path, sm_inst,
                   ds_ids, moldb_name='HMDB-v4', adducts=DEFAULT_ADDUCTS,
                   fdr=0.5, overwrite=False):
    pack_moldb(sm_inst, moldb_name, data_path, adducts)

    pack_datasets(image_api_endpoint, data_path, sm_inst, ds_ids,
                  moldb_name, fdr=fdr, overwrite=overwrite)