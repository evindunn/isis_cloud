import connexion


class ISISServer(connexion.FlaskApp):
    def __init__(self):
        super().__init__(
            __name__,
            specification_dir="openapi",
            options={"swagger_url": "/docs"}
        )
        self.add_api("main.yml")
