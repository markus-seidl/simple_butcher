from tqdm import tqdm


class ProgressBarManager:
    def __init__(self):
        self.pbars = dict()

    def create(self, name: str) -> tqdm:
        ret = tqdm(
            ncols=120,
            position=len(self.pbars),
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False
        )
        self.pbars[name] = ret
        return ret

    def close(self, name):
        if name not in self.pbars:
            return

        temp = self.pbars[name]
        del self.pbars[name]

        try:
            temp.close()
        except:
            pass

    def close_all(self):
        for k in self.pbars:
            self.close(k)

# class ProgressBar:
#     def __init__(self, manager: "ProgressBarManager", t: tqdm):
#         self.manager = manager
#         self.t = t
#
#     def __enter__(self):
#         self.t.__enter__()
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.t.__exit__(exc_type, exc_val, exc_tb)
