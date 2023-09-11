from dwh.adapters.titanic_ds_adapter import TitanicDataSetAdapter


class Test_TitanicDataSetAdapter:
    def test_download(self):
        adapter = TitanicDataSetAdapter("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
        passengers = adapter.load()
        assert passengers is not None
        assert len(passengers) == 887

        check_passenger = next((p for p in passengers if p.name == "Mr. Owen Harris Braund"), None)
        assert check_passenger is not None
        assert check_passenger.age == 22.0
        assert check_passenger.survived is True
        assert check_passenger.p_class == 3
