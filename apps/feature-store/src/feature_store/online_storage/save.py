from hopeit.app.context import EventContext

from feature_store.datamodel import FeatureValue

__steps__ = ["dummy"]


def dummy(payload: FeatureValue, context: EventContext) -> None:
    pass
