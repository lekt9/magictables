from .chain import Chain
from .shadow_db import ShadowDB


class Explain:
    def __init__(self, chain: Chain):
        self.chain = chain
        self.shadow_db = ShadowDB.get_instance()

    def summary(self):
        return f"Chain with {len(self.chain.steps)} steps"

    def data_flow(self):
        return "\n".join(
            [f"Step {i+1}: {step.name}" for i, step in enumerate(self.chain.steps)]
        )

    def parsing_logic(self):
        return "AI-driven parsing logic based on provided queries"

    def shadow_db_schema(self):
        return self.shadow_db.get_schema()
