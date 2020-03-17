import styled from 'styled-components';
import { Label } from '@components/common/typography';

export const Container = styled.div`
  text-align: start;
`;

export const ButtonText = styled(Label)`
  margin-left: 10px;
  color: ${props => props.color || props.theme.color.primary300};
`;
