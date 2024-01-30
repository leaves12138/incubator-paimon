/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.paimon.secondaryindex;
//
// import org.apache.paimon.filter.FilterInterface;
// import org.apache.paimon.predicate.CompoundPredicate;
// import org.apache.paimon.predicate.FieldRef;
// import org.apache.paimon.predicate.FunctionVisitor;
// import org.apache.paimon.predicate.LeafPredicate;
// import org.apache.paimon.predicate.Or;
// import org.apache.paimon.predicate.Predicate;
// import org.apache.paimon.predicate.PredicateVisitor;
//
// import java.util.List;
//
// import static org.apache.paimon.secondaryindex.InternalRowToBytesVisitor.NULL_BYTES;
//
// public class PredicateTester implements PredicateVisitor<Boolean> {
//
//    public PredicateTester(FilterInterface filterInterface) {}
//
//    private Visitor2 visitor2;
//
//    @Override
//    public Boolean visit(LeafPredicate predicate) {
//
//        return predicate
//                .function()
//                .visit(
//                        visitor2,
//                        new FieldRef(predicate.index(), predicate.fieldName(), predicate.type()),
//                        predicate.literals());
//    }
//
//    @Override
//    public Boolean visit(CompoundPredicate predicate) {
//
//        if (predicate.function() instanceof Or) {
//            for (Predicate predicate1 : predicate.children()) {
//                if (predicate1.visit(this)) {
//                    return true;
//                }
//            }
//            return false;
//
//        } else {
//            for (Predicate predicate1 : predicate.children()) {
//                if (!predicate1.visit(this)) {
//                    return false;
//                }
//            }
//            return true;
//        }
//    }
//
//    class Visitor2 implements FunctionVisitor<Boolean> {
//
//        public Visitor2(FilterInterface filterInterface) {
//
//            this.filterInterface = filterInterface;
//            this.objectToBytesVisitor = new ObjectToBytesVisitor();
//        }
//
//        private FilterInterface filterInterface;
//        private ObjectToBytesVisitor objectToBytesVisitor;
//
//        @Override
//        public Boolean visitIsNotNull(FieldRef fieldRef) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitIsNull(FieldRef fieldRef) {
//            return filterInterface.test(NULL_BYTES);
//        }
//
//        @Override
//        public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitEqual(FieldRef fieldRef, Object literal) {
//            return filterInterface.test(
//                    fieldRef.type().accept(objectToBytesVisitor).apply(literal));
//        }
//
//        @Override
//        public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
//            for (Object o : literals) {
//                if (filterInterface.test(fieldRef.type().accept(objectToBytesVisitor).apply(o))) {
//                    return true;
//                }
//            }
//            return false;
//        }
//
//        @Override
//        public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitAnd(List<Boolean> children) {
//            return true;
//        }
//
//        @Override
//        public Boolean visitOr(List<Boolean> children) {
//            return true;
//        }
//    }
// }
