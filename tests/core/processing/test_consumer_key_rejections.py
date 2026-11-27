"""A consumer's join key rejects a NULL, and where its other side holds only
the concept's own values, any value outside the concept: the second reading
is the stronger one (`ConsumerProofs.keys`)."""

from trilogy.core.processing.consumer_proofs import ConsumerProofs

NULL_ONLY = ConsumerProofs(keys={"k": False})
OWN = ConsumerProofs(keys={"k": True})


def test_siblings_of_one_row_set_keep_the_stricter_reading():
    assert (NULL_ONLY | OWN).keys == {"k": True}
    assert (NULL_ONLY | ConsumerProofs(keys={"j": True})).keys == {
        "k": False,
        "j": True,
    }


def test_every_consumer_must_hold_a_key_to_its_own_values():
    assert (NULL_ONLY & OWN).keys == {"k": False}
    assert (OWN & OWN).keys == {"k": True}
    assert (OWN & ConsumerProofs(keys={"j": True})).keys == {}


def test_restrict_keeps_each_key_as_read():
    proofs = ConsumerProofs(keys={"k": True, "j": False})
    assert proofs.restrict({"k"}).keys == {"k": True}


def test_a_weaker_reading_no_longer_covers_what_a_merge_took():
    assert OWN.covers(NULL_ONLY)
    assert OWN.covers(OWN)
    assert not NULL_ONLY.covers(OWN)
    assert not ConsumerProofs().covers(NULL_ONLY)
    assert NULL_ONLY.covers(ConsumerProofs())


def test_a_key_is_a_proof():
    assert NULL_ONLY
    assert not ConsumerProofs()
