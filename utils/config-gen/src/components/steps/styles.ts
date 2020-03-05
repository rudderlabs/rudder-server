import styled from 'styled-components';
import { Flex } from '@components/common/misc';
import { Dot } from '@components/common/dot';

export const StyledSteps = styled.div`
  background: ${({ theme }) => theme.color.white};
  box-shadow: 0px 2px 10px rgba(0, 0, 0, 0.15);
  border-radius: 6px;
`;

export const StyledStepsFooter = styled(Flex)`
  height: 84px;
  padding: 0 30px;
  border-top: 1px solid ${({ theme }) => theme.color.grey100};
`;

export const DotsContainer = styled(Flex)`
  ${Dot} {
    margin-right: 8px;
  }
`;
