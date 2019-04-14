import argparse
from pathlib import Path

from metaspace.sm_annotation_utils import SMInstance
from pack_data import pack_metaspace

sm_host = 'https://metaspace2020.eu'
image_api_endpoint = 'https://metaspace2020.eu'
moldb_name = 'HMDB-v4'
adducts = ['+H', '+Na', '+K', '-H', '+Cl']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run METASPACE pixel export')
    parser.add_argument('--ds-ids', dest='ds_ids', default=None, help='DS id (or comma-separated list of ids)')
    parser.add_argument('--ds-name', dest='ds_name', default=None, help="DS name mask('' will match all datasets)")
    parser.add_argument('--export-path', dest='data_path', default='./export',
                        help='Path where to export the data to')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite already exported datasets')
    args = parser.parse_args()
    assert not (args.ds_ids and args.ds_name)

    data_path = Path(args.data_path)
    data_path.mkdir(parents=True, exist_ok=True)
    sm_inst = SMInstance(host=sm_host)

    if args.ds_ids:
        ds_ids = args.ds_ids.split(',') if args.ds_ids else []
    else:
        dss = sm_inst.datasets(nameMask=args.ds_name)
        ds_ids = [ds.id for ds in dss]

    pack_metaspace(image_api_endpoint, data_path, sm_inst,
                   ds_ids, overwrite=args.overwrite)
