from sqlalchemy import Column, String, DateTime

from base import Base


class User(Base):
    """
    User class for user model.
    """

    __tablename__ = "user"

    uuid = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, unique=True)
    password = Column(String)
    role = Column(String)
    created_at = Column(DateTime)
    modified_at = Column(DateTime)

    def as_dict(self):

        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }
