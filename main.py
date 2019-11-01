from DataImport import DataImport


def main():
    # region prepare
    _is_print_elapsed = False
    default_directories = {'tracks': '.\\Files\\unique_tracks.txt', 'triplets': '.\\Files\\triplets_sample_20p.txt'}
    # endregion
    # region import
    data = DataImport(default_directories['tracks'], '<sep>')
    data.create_engine(print_elapsed=_is_print_elapsed)
    # endregion
    input("\nPress any button to end...\t")


if __name__ == "__main__":
    main()
