from aiogram.fsm.state import State, StatesGroup


class AdminStates(StatesGroup):
    creating_node_id = State()
    creating_node_text = State()
    editing_text = State()
    adding_type = State()
    adding_text = State()
    adding_target = State()
    adding_row = State()
    linking_target = State()
    attaching_media = State()
    delete_confirm = State()
