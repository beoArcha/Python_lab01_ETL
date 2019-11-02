from DataImport import DataImport
from Tools import elapsed


@elapsed
def main(**kwargs):
    # region prepare
    _is_print_elapsed = True
    default_directories = {'tracks': '.\\Files\\unique_tracks.txt', 'triplets': '.\\Files\\triplets_sample_20p.txt'}
    # endregion
    # region import tracks
    data = DataImport(default_directories['tracks'], '<SEP>')
    data.create_engine(print_elapsed=_is_print_elapsed)
    data.data_import('tracks', print_elapsed=_is_print_elapsed)
    # endregion
    # region import triplets
    # data.address = default_directories['triplets']
    # data.data_import('triplets', print_elapsed=_is_print_elapsed)
    # endregion
    data.disconnect_engine()
    input("\nPress any button to end...\t")


if __name__ == "__main__":
    main(print_elapsed=True)
