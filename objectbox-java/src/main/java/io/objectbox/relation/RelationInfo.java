/*
 * Copyright 2017 ObjectBox Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.objectbox.relation;

import java.io.Serializable;

import javax.annotation.concurrent.Immutable;

import io.objectbox.EntityInfo;
import io.objectbox.Property;
import io.objectbox.annotation.apihint.Internal;
import io.objectbox.internal.ToManyGetter;
import io.objectbox.internal.ToOneGetter;

@Internal
@Immutable
/**
 * Meta info describing a relation including source and target entity.
 */
public class RelationInfo<TARGET> implements Serializable {
    private static final long serialVersionUID = 7412962174183812632L;

    public final EntityInfo sourceInfo;
    public final EntityInfo<TARGET> targetInfo;

    /** For relations based on a target ID property (null for stand-alone relations). */
    public final Property targetIdProperty;

    /** Only set for ToOne relations */
    public final ToOneGetter toOneGetter;

    /** Only set for ToMany relations */
    public final ToManyGetter toManyGetter;

    /** For ToMany relations based on backlinks (null for stand-alone relations). */
    public final ToOneGetter backlinkToOneGetter;

    /** For stand-alone to-many relations (0 otherwise). */
    public final int relationId;

    /**
     * ToOne
     */
    public RelationInfo(EntityInfo sourceInfo, EntityInfo<TARGET> targetInfo, Property targetIdProperty,
                        ToOneGetter toOneGetter) {
        this.sourceInfo = sourceInfo;
        this.targetInfo = targetInfo;
        this.targetIdProperty = targetIdProperty;
        this.toOneGetter = toOneGetter;
        this.backlinkToOneGetter = null;
        this.toManyGetter = null;
        this.relationId = 0;
    }

    /**
     * ToMany as a ToOne backlink
     */
    public RelationInfo(EntityInfo sourceInfo, EntityInfo<TARGET> targetInfo, ToManyGetter toManyGetter,
                        Property targetIdProperty, ToOneGetter backlinkToOneGetter) {
        this.sourceInfo = sourceInfo;
        this.targetInfo = targetInfo;
        this.targetIdProperty = targetIdProperty;
        this.toManyGetter = toManyGetter;
        this.backlinkToOneGetter = backlinkToOneGetter;
        this.toOneGetter = null;
        this.relationId = 0;
    }

    /**
     * Stand-alone ToMany.
     */
    public RelationInfo(EntityInfo sourceInfo, EntityInfo<TARGET> targetInfo, ToManyGetter toManyGetter,
                        int relationId) {
        this.sourceInfo = sourceInfo;
        this.targetInfo = targetInfo;
        this.relationId = relationId;
        this.toManyGetter = toManyGetter;
        this.targetIdProperty = null;
        this.toOneGetter = null;
        this.backlinkToOneGetter = null;
    }

    @Override
    public String toString() {
        return "RelationInfo from " + sourceInfo.getEntityClass() + " to " + targetInfo.getEntityClass();
    }
}

