import { useRecoilCallback } from 'recoil';

import { useRecordTableStates } from '@/object-record/record-table/hooks/internal/useRecordTableStates';
import { useSetTableCellStatus } from '@/object-record/record-table/scopes/TableStatusSelectorContext';
import { getSnapshotValue } from '@/ui/utilities/recoil-scope/utils/getSnapshotValue';

import { TableCellPosition } from '../../types/TableCellPosition';

export const useSetSoftFocusPosition = (recordTableId?: string) => {
  const {
    softFocusPositionState,
    isSoftFocusActiveState,
    isSoftFocusOnTableCellFamilyState,
  } = useRecordTableStates(recordTableId);

  const setTableCellStatus = useSetTableCellStatus();

  return useRecoilCallback(
    ({ set, snapshot }) => {
      return (newPosition: TableCellPosition) => {
        const currentPosition = getSnapshotValue(
          snapshot,
          softFocusPositionState,
        );

        set(isSoftFocusActiveState, true);

        set(isSoftFocusOnTableCellFamilyState(currentPosition), false);

        setTableCellStatus(
          currentPosition.row,
          currentPosition.column,
          (currentTableCellStatus) => ({
            ...currentTableCellStatus,
            hasSoftFocus: false,
          }),
        );

        set(softFocusPositionState, newPosition);

        set(isSoftFocusOnTableCellFamilyState(newPosition), true);

        setTableCellStatus(
          newPosition.row,
          newPosition.column,
          (currentTableCellStatus) => ({
            ...currentTableCellStatus,
            hasSoftFocus: true,
          }),
        );
      };
    },
    [
      setTableCellStatus,
      softFocusPositionState,
      isSoftFocusActiveState,
      isSoftFocusOnTableCellFamilyState,
    ],
  );
};
