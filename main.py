import os
from DataImport import DataImport
from Tools import elapsed


def create(is_print_elapsed: bool,
           _is_test: bool,
           default_directories: dict,
           default_table_columns_names: dict) -> None:
    data = DataImport('<SEP>', False, _is_test)
    data.create_engine(print_elapsed=is_print_elapsed)
    for key, value in default_directories.items():
        data.address = value
        data.columns_name_list = default_table_columns_names[key]
        data.data_import(key, print_elapsed=is_print_elapsed)
    data.disconnect_engine()


def print_info(is_print_elapsed: bool) -> None:
    most_popular_artist = 'SELECT artist, COUNT(artist) as performances '\
                          'FROM tracks JOIN triplets ON tracks.track_id = triplets.track_id '\
                          'GROUP BY artist ORDER BY performances DESC LIMIT 1'
    five_most_popular_songs = 'SELECT title, COUNT(title) as performances ' \
                              'FROM tracks JOIN triplets ON tracks.track_id = triplets.track_id ' \
                              'GROUP BY title ORDER BY performances DESC LIMIT 5'
    data = DataImport('')
    print('Most popular artist: ')
    print_result(data.execute(most_popular_artist, print_elapsed=is_print_elapsed), '{0}. {1} -- {2}')
    print('Five most popular songs: ')
    print_result(data.execute(five_most_popular_songs, print_elapsed=is_print_elapsed), '{0}. {1} -- {2}')


def print_result(result: list, text: str) -> None:
    for e, r in enumerate(result):
        print(text.format(e + 1, *r))


@elapsed
def main(**kwargs) -> None:
    # region prepare
    _is_print_elapsed = True
    _is_test = True
    _dir = os.path.dirname(__file__)
    default_directories = {'tracks': '{}\\Files\\unique_tracks.txt'.format(_dir),
                           'triplets': '{}\\Files\\triplets_sample_20p.txt'.format(_dir)}
    default_table_columns_names = {'tracks': ['performance_id', 'track_id', 'artist', 'title'],
                                   'triplets': ['user_id', 'track_id', 'date']}
    # endregion
    # region execution
    create(_is_print_elapsed, _is_test, default_directories, default_table_columns_names)
    print_info(_is_print_elapsed)
    # endregion


if __name__ == "__main__":
    main(print_elapsed=True)
    input("\nPress any button to end...\t")
