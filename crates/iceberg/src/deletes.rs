use roaring::RoaringBitmap;
use arrow_array::RecordBatch;

// Represents a parsed Delete file that can be safely stored
// in the Object Cache.
pub(crate) enum Deletes {
    // Positional delete files are parsed into a map of
    // filename to a sorted list of row indices.

    // TODO: Ignoring the stored rows that are present in
    //   positional deletes for now. I think they only used for statistics?
    Vector(RoaringBitmap),

    // Equality delete files are initially parsed solely as an
    // unprocessed list of `RecordBatch`es from the equality
    // delete files.
    // I don't think we can do better than this by
    // storing a Predicate (because the equality deletes use the
    // field_id rather than the field name, so if we keep this as
    // a Predicate then a field name change would break it).
    // Similarly, I don't think we can store this as a BoundPredicate
    // as the column order could be different across different data
    // files and so the accessor in the bound predicate could be invalid).
    Equality(Vec<RecordBatch>),
}
