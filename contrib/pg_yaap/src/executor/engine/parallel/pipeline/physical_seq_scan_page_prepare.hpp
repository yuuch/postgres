#pragma once

template <bool AllVisible, bool CheckSerializable>
pg_attribute_always_inline static int
PgYaapCollectPageTuples(HeapScanDesc scan,
						 Snapshot snapshot,
						 Page page,
						 Buffer buffer,
						 BlockNumber block,
						 int lines)
{
	Oid			relid = RelationGetRelid(scan->rs_base.rs_rd);
	int			ntup = 0;
	int			nvis = 0;
	BatchMVCCState batchmvcc;

	Assert(IsMVCCSnapshot(snapshot));

	for (OffsetNumber lineoff = FirstOffsetNumber; lineoff <= lines; lineoff++)
	{
		ItemId		lpp = PageGetItemId(page, lineoff);
		HeapTuple	tup;

		if (unlikely(!ItemIdIsNormal(lpp)))
			continue;

		if constexpr (!AllVisible || CheckSerializable)
		{
			tup = &batchmvcc.tuples[ntup];
			tup->t_data = (HeapTupleHeader) PageGetItem(page, lpp);
			tup->t_len = ItemIdGetLength(lpp);
			tup->t_tableOid = relid;
			ItemPointerSet(&(tup->t_self), block, lineoff);
		}

		if constexpr (AllVisible)
		{
			if constexpr (CheckSerializable)
				batchmvcc.visible[ntup] = true;
			scan->rs_vistuples[ntup] = lineoff;
		}

		ntup++;
	}

	Assert(ntup <= MaxHeapTuplesPerPage);

	if constexpr (AllVisible)
		nvis = ntup;
	else
		nvis = HeapTupleSatisfiesMVCCBatch(snapshot, buffer,
										   ntup,
										   &batchmvcc,
										   scan->rs_vistuples);

	if constexpr (CheckSerializable)
	{
		for (int i = 0; i < ntup; i++)
		{
			HeapCheckForSerializableConflictOut(batchmvcc.visible[i],
												scan->rs_base.rs_rd,
												&batchmvcc.tuples[i],
												buffer,
												snapshot);
		}
	}

	return nvis;
}
