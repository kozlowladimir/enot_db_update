from pathlib import Path
from loguru import logger

from config import PATH_TO_REPOSITORY_DATA, connection_params
from providers import UhhtBarcodeReference, UhhtBarcodeReferenceRemote, WriterProvider


def main():
    """Главная функция для апдейта базы"""
    with open('current_commit_hash', 'r') as file:
        current_commits_hash = file.read()

    try:
        with open('last_commit_hash', 'r') as file:
            last_commits_hash = file.read()
    except FileNotFoundError:
        last_commits_hash = ''

    if current_commits_hash == last_commits_hash:
        logger.debug('Nothing to update. Commits hashes are equal.')
        return 0

    init_path = Path(PATH_TO_REPOSITORY_DATA)

    uhht_barcode_reference = UhhtBarcodeReference(init_path)
    uhht_barcode_reference_remote = UhhtBarcodeReferenceRemote(connection_params)

    local_barcodes = uhht_barcode_reference.get_barcodes()
    remote_barcodes = uhht_barcode_reference_remote.get_barcodes()

    new_barcodes = local_barcodes - remote_barcodes
    if len(new_barcodes) > 0:
        logger.debug('There are {0} new barcodes'.format(len(new_barcodes)))

        new_table = uhht_barcode_reference.get_table_by_barcodes(new_barcodes)
        writer_provider = WriterProvider(connection_params)
        writer_provider.insert_into_db(new_table)
    else:
        logger.debug('There are no new barcodes')

    with open('last_commit_hash', 'w') as file:
        file.write("{0}".format(current_commits_hash))

    return 0


if __name__ == '__main__':
    main()
