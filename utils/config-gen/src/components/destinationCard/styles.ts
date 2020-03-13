import { Card } from '@components/common/card';
import { Label } from '@components/common/typography';
import styled from 'styled-components';

import { Text } from '../common/typography';

export const StyledCard = styled(Card)`
  min-width: 371px;
  border-radius: 20px;
  padding-left: 0px;
  padding-right: 35px;
  margin-bottom: 10px;
  display: flex;
  align-items: center;
  box-shadow: none;
  height: 100px;
`;

export const Content = styled.div`
  padding: 0px 0px 0px 20px;
`;

export const EnabledText = styled(Text)`
  margin: 5px;
`;

export const EmptyStyledCard = styled(StyledCard)`
  border: 3px solid #e0e0e0;
  background-color: #f0f2f5;
  height: auto;
`;

export const ButtonText = styled(Label)`
  margin-left: 10px;
  color: ${props => props.color || props.theme.color.primary300};
`;
