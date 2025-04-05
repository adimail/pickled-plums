from sqlalchemy import Column, String, Text, ForeignKey, DateTime

from base import Base


class Client(Base):
    """
    ClientMachine class model for storing machine information.
    """

    __tablename__ = "client_machine"

    uuid = Column(String, primary_key=True)
    name = Column(String)
    description = Column(Text)
    mac_address = Column(String)
    public_ip = Column(String)
    internal_ip = Column(String)
    created_at = Column(DateTime)
    last_active = Column(DateTime)
    user = Column(String, ForeignKey("user.uuid"), nullable=False)
