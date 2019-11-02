from DataImport import DataImport
from Tools import elapsed


def create(_is_print_elapsed, _is_test, default_directories, default_table_columns_names):
    # region import tracks
    data = DataImport(default_directories['tracks'], '<SEP>', False, _is_test)
    data.create_engine(print_elapsed=_is_print_elapsed)
    data.columns_name_list = default_table_columns_names['tracks']
    data.data_import('tracks', print_elapsed=_is_print_elapsed)
    # endregion
    # region import triplets
    data.address = default_directories['triplets']
    data.columns_name_list = default_table_columns_names['triplets']
    data.data_import('triplets', print_elapsed=_is_print_elapsed)
    # endregion
    data.disconnect_engine()


def print_info():
    pass


@elapsed
def main(**kwargs):
    # region prepare
    _is_print_elapsed = True
    _is_test = True
    default_directories = {'tracks': '.\\Files\\unique_tracks.txt', 'triplets': '.\\Files\\triplets_sample_20p.txt'}
    default_table_columns_names = {'tracks': ['track_id', 'performance_id', 'artist', 'title'],
                                   'triplets': ['user_id', 'track_id', 'date']}
    # endregion
    create(_is_print_elapsed, _is_test, default_directories, default_table_columns_names)
    print_info()
    input("\nPress any button to end...\t")


if __name__ == "__main__":
    main(print_elapsed=True)
