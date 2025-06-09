from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models import Base


class PublicationStatus(Base):
    __tablename__ = "publication_statuses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(unique=True)
    name: Mapped[str]
    sections: Mapped[list["Section"]] = relationship(back_populates="status")
    subsections: Mapped[list["Subsection"]] = relationship(back_populates="status")


class Section(Base):
    __tablename__ = "sections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    status_id: Mapped[int] = mapped_column(ForeignKey("publication_statuses.id"))
    status: Mapped["PublicationStatus"] = relationship(back_populates="sections")
    subsections: Mapped[list["Subsection"]] = relationship(back_populates="section")


class Subsection(Base):
    __tablename__ = "subsections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    section_id: Mapped[int] = mapped_column(ForeignKey("sections.id", ondelete="CASCADE"))
    section: Mapped["Section"] = relationship(back_populates="subsections")
    status_id: Mapped[int] = mapped_column(ForeignKey("publication_statuses.id", ondelete="CASCADE"))
    status: Mapped["PublicationStatus"] = relationship(back_populates="subsections")
    article_contents: Mapped[list["ArticleContent"]] = relationship(
        back_populates="subsection"
    )


class Widget(Base):
    __tablename__ = "widgets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    code: Mapped[str] = mapped_column(unique=True)
    article_contents: Mapped[list["ArticleContent"]] = relationship(
        back_populates="widget"
    )


class ArticleContent(Base):
    __tablename__ = "article_contents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subtitle: Mapped[str]
    text: Mapped[str]
    subsection_id: Mapped[int] = mapped_column(ForeignKey("subsections.id", ondelete="CASCADE"))
    subsection: Mapped["Subsection"] = relationship(back_populates="article_contents")
    widget_id: Mapped[int] = mapped_column(ForeignKey("widgets.id"))
    widget: Mapped["Widget"] = relationship(back_populates="article_contents")
