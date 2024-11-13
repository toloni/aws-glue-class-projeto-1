class DateToProcess:
    def __init__(self):
        self._data = {}

    def add_dates(self, base: str, last_date: int, prev_date: int):
        if base not in self._data:
            self._data[base] = []
        self._data[base].append((last_date, prev_date))

    def get_dates(self, base: str):
        return self._data.get(base, [])

    def get_unique_dates(self):
        unique_dates = set()
        for dates in self._data.values():
            for last_date, prev_date in dates:
                unique_dates.add(last_date)
                unique_dates.add(prev_date)
        return list(unique_dates)

    def get_last_date(self, base: str):
        dates = self.get_dates(base)
        return max(dates, key=lambda x: x[0])[0] if dates else None

    def get_prev_date(self, base: str):
        dates = self.get_dates(base)
        return max(dates, key=lambda x: x[1])[1] if dates else None
