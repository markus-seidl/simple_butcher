import typing
from tqdm import tqdm
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn, \
    TextColumn, BarColumn, TaskProgressColumn, FileSizeColumn, TotalFileSizeColumn, TaskID, TransferSpeedColumn


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
            leave=False,
            miniters=1,
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
        temp = list(self.pbars.keys())
        for k in temp:
            self.close(k)


class ProgressDisplay:
    def __init__(self):
        self.progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            FileSizeColumn(),
            TextColumn("/"),
            TotalFileSizeColumn(),
            TransferSpeedColumn(),
            TextColumn("{task.fields[postfix]}"),
        )
        self.progress.start()

    def create_byte_bar(self, name, total_bytes: int, postfix: str = "") -> "ByteTask":
        ret = self.progress.add_task(
            name, total=total_bytes, postfix=postfix
        )

        return ByteTask(self, ret)

    def create_tape_bar(self, tape_capacity: int, tape_serial: str) -> "ByteTask":
        return ByteTask(self, self.progress.add_task(
            "tape", total=tape_capacity, postfix=f"serial={tape_serial}"
        ))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.__exit__(exc_type, exc_val, exc_tb)


class ByteTask:
    def __init__(self, parent: ProgressDisplay, task_id: TaskID):
        self.parent = parent
        self.task_id = task_id

    def update(
            self,
            total: typing.Optional[float] = None,
            completed: typing.Optional[float] = None,
            advance: typing.Optional[float] = None,
            description: typing.Optional[str] = None,
            visible: typing.Optional[bool] = None,
            refresh: bool = False,
            **fields: typing.Any,
    ):
        self.parent.progress.update(
            self.task_id, total=total, completed=completed, advance=advance, description=description,
            visible=visible, refresh=refresh, **fields
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.parent.progress.update(self.task_id, visible=False)
        self.parent.progress.stop_task(self.task_id)
