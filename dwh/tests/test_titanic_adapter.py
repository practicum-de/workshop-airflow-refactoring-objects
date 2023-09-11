from dwh.core.adapters.titanic_adapter import TitanicApiAdapter


class Test_TitanicApiAdapter:
    def test_download(self):
        adapter = TitanicApiAdapter("https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv")
        data = adapter.download()
        assert data is not None
        assert len(data) == 887

        check_person = next((p for p in data if p.name == "Mr. Edward H Wheadon"), None)
        assert check_person is not None
        assert check_person.survived is True
        assert check_person.p_class == 2
        assert check_person.name == "Mr. Edward H Wheadon"
        assert check_person.sex == "male"
        assert check_person.age == 66.0
        assert check_person.fare == 10.5
        assert check_person.parents_children_aboard == 0
        assert check_person.siblings_spouses_aboard == 0
