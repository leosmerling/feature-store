from hopeit.app.context import EventContext

from feature_store.datamodel import FeatureValues

__steps__ = ["dummy"]


def dummy(payload: FeatureValues, context: EventContext) -> None:
    pass
