package io.objectbox.tree;

import io.objectbox.BoxStore;
import io.objectbox.Cursor;
import io.objectbox.annotation.apihint.Internal;
import io.objectbox.internal.CursorFactory;
import io.objectbox.relation.ToOne;

// THIS CODE IS GENERATED BY ObjectBox, DO NOT EDIT.

/**
 * ObjectBox generated Cursor implementation for "DataBranch".
 * Note that this is a low-level class: usually you should stick to the Box class.
 */
public final class DataBranchCursor extends Cursor<DataBranch> {
    @Internal
    static final class Factory implements CursorFactory<DataBranch> {
        @Override
        public Cursor<DataBranch> createCursor(io.objectbox.Transaction tx, long cursorHandle, BoxStore boxStoreForEntities) {
            return new DataBranchCursor(tx, cursorHandle, boxStoreForEntities);
        }
    }

    private static final DataBranch_.DataBranchIdGetter ID_GETTER = DataBranch_.__ID_GETTER;


    private final static int __ID_uid = DataBranch_.uid.id;
    private final static int __ID_parentId = DataBranch_.parentId.id;
    private final static int __ID_metaBranchId = DataBranch_.metaBranchId.id;

    public DataBranchCursor(io.objectbox.Transaction tx, long cursor, BoxStore boxStore) {
        super(tx, cursor, DataBranch_.__INSTANCE, boxStore);
    }

    @Override
    public final long getId(DataBranch entity) {
        return ID_GETTER.getId(entity);
    }

    /**
     * Puts an object into its box.
     *
     * @return The ID of the object within its box.
     */
    @Override
    public final long put(DataBranch entity) {
        ToOne<DataBranch> parent = entity.parent;
        if(parent != null && parent.internalRequiresPutTarget()) {
            Cursor<DataBranch> targetCursor = getRelationTargetCursor(DataBranch.class);
            try {
                parent.internalPutTarget(targetCursor);
            } finally {
                targetCursor.close();
            }
        }
        ToOne<MetaBranch> metaBranch = entity.metaBranch;
        if(metaBranch != null && metaBranch.internalRequiresPutTarget()) {
            Cursor<MetaBranch> targetCursor = getRelationTargetCursor(MetaBranch.class);
            try {
                metaBranch.internalPutTarget(targetCursor);
            } finally {
                targetCursor.close();
            }
        }
        String uid = entity.uid;
        int __id1 = uid != null ? __ID_uid : 0;

        long __assignedId = collect313311(cursor, entity.id, PUT_FLAG_FIRST | PUT_FLAG_COMPLETE,
                __id1, uid, 0, null,
                0, null, 0, null,
                __ID_parentId, entity.parent.getTargetId(), __ID_metaBranchId, entity.metaBranch.getTargetId(),
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0);

        entity.id = __assignedId;

        attachEntity(entity);
        return __assignedId;
    }

    private void attachEntity(DataBranch entity) {
        // Transformer will create __boxStore field in entity and init it here:
        // entity.__boxStore = boxStoreForEntities;
    }

}